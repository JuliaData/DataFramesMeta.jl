function addkey!(membernames, nam)
    if !haskey(membernames, nam)
        membernames[nam] = gensym()
    end
    membernames[nam]
end

onearg(e::Expr, f) = e.head == :call && length(e.args) == 2 && e.args[1] == f
onearg(e, f) = false

"""
    get_column_expr(x)

If the input is a valid column identifier, i.e.
a `QuoteNode` or an expression beginning with
`$DOLLAR`, returns the underlying identifier.

If input is not a valid column identifier,
returns `nothing`.
"""
get_column_expr(x; allow_multicol::Bool = false) = nothing
function get_column_expr(e::Expr; allow_multicol::Bool = false)
    e.head == :$ && return e.args[1]
    onearg(e, :AsTable) && return :($AsTable($(e.args[2])))
    if onearg(e, :cols)
        Base.depwarn("cols is deprecated use $DOLLAR to escape column names instead", :cols)
        return e.args[2]
    end
    if e.head === :call
        e1 = e.args[1]
        if e1 === :All || e1 === :Not || e1 === :Between || e1 == :Cols
            if allow_multicol
                return e
            else
                s = "Multi-column references outside of @select, @rselect, @select!" *
                 " and @rselect! must be wrapped in AsTable"
                throw(ArgumentError(s))
            end
        end
    end
    return nothing
end
get_column_expr(x::QuoteNode; allow_multicol::Bool = false) = x

get_column_expr_rename(x) = nothing
function get_column_expr_rename(e::Expr)
    e.head == :$ && return e.args[1]
    return nothing
end
get_column_expr_rename(x::QuoteNode) = x

mapexpr(f, e) = Expr(e.head, Base.Generator(f, e.args)...)

replace_syms!(membernames, x) = x
replace_syms!(membernames, q::QuoteNode) = addkey!(membernames, q)

function replace_syms!(membernames, e::Expr)
    if onearg(e, :^)
        return e.args[2]
    end

    col = get_column_expr(e)
    if col !== nothing
        return addkey!(membernames, col)
    elseif e.head == :.
        return replace_dotted!(membernames, e)
    else
        return mapexpr(x -> replace_syms!(membernames, x), e)
    end
end

protect_replace_syms!(membernames, e) = e
protect_replace_syms!(membernames, e::Expr) = replace_syms!(membernames, e)

function replace_dotted!(membernames, e)
    x_new = replace_syms!(membernames, e.args[1])
    y_new = protect_replace_syms!(membernames, e.args[2])
    Expr(:., x_new, y_new)
end

composed_or_symbol(x) = false
composed_or_symbol(x::Symbol) = true
function composed_or_symbol(x::Expr)
    x.head == :call &&
        x.args[1] == :∘ &&
        all(composed_or_symbol, x.args[2:end])
end

is_call(x) = false
is_call(x::Expr) = x.head === :call

is_nested_fun(x) = false
function is_nested_fun(x::Expr)
    x.head === :call &&
    length(x.args) == 2 &&
    is_call(x.args[2]) &&
    # Don't count `^(x)`
    onearg(x, :^) == false &&
    # AsTable(:x) or `$(:x)`
    return get_column_expr(x.args[2]) === nothing
end

is_nested_fun_recursive(x, nested_once) = false
function is_nested_fun_recursive(x::Expr, nested_once)
    if is_nested_fun(x)
        return is_nested_fun_recursive(x.args[2], true)
    elseif is_simple_non_broadcast_call(x)
        return nested_once
    else
        return false
    end
end

fix_simple_dot(x) = x
function fix_simple_dot(x::Symbol)
    if startswith(string(x), '.')
        f_sym_without_dot = Symbol(chop(string(x), head = 1, tail = 0))
        return Expr(:., f_sym_without_dot)
    else
        return x
    end
end

make_composed(x) = x
function make_composed(x::Expr)
    funs = Any[]
    x_orig = x
    nested_once = false
    while true
        fun = fix_simple_dot(x.args[1])
        if is_nested_fun(x)
            push!(funs, fun)
            x = x.args[2]
            nested_once = true
        elseif is_simple_non_broadcast_call(x) && nested_once
            push!(funs, fun)
            # ∘(f, g, h)(:x, :y, :z)
            x = Expr(:call, Expr(:call, ∘, funs...), x.args[2:end]...)
            return x
        else
            throw(ArgumentError("Not eligible for function composition"))
        end
    end
end


is_simple_non_broadcast_call(x) = false
function is_simple_non_broadcast_call(expr::Expr)
    expr.head == :call &&
        length(expr.args) >= 2 &&
        onearg(expr, :^) == false &&
        composed_or_symbol(expr.args[1]) &&
        all(a -> get_column_expr(a) !== nothing, expr.args[2:end])
end

is_simple_broadcast_call(x) = false
function is_simple_broadcast_call(expr::Expr)
    expr.head == :. &&
        length(expr.args) == 2 &&
        composed_or_symbol(expr.args[1]) &&
        expr.args[2] isa Expr &&
        expr.args[2].head == :tuple &&
        all(a -> get_column_expr(a) !== nothing, expr.args[2].args)
end

function args_to_selectors(v)
    t = Base.Generator(v) do arg
        col = get_column_expr(arg)
        col === nothing && throw(ArgumentError("This path should not be reached, arg: $(arg)"))
        col
    end

    :($make_source_concrete($(Expr(:vect, t...))))
end

is_macro_head(ex, name) = false
is_macro_head(ex::Expr, name) = ex.head == :macrocall && ex.args[1] == Symbol(name)

const BYROW_SYM = Symbol("@byrow")
const PASSMISSING_SYM = Symbol("@passmissing")
const ASTABLE_SYM = Symbol("@astable")
const DEFAULT_FLAGS = (;BYROW_SYM => Ref(false), PASSMISSING_SYM => Ref(false), ASTABLE_SYM => Ref(false))

extract_macro_flags(ex, exprflags = deepcopy(DEFAULT_FLAGS)) = (ex, exprflags)
function extract_macro_flags(ex::Expr, exprflags = deepcopy(DEFAULT_FLAGS))
    if ex.head == :macrocall
        macroname = ex.args[1]
        if macroname in keys(exprflags)
            exprflag = exprflags[macroname]
            if exprflag[] == true
                throw(ArgumentError("Redundant flag $macroname used."))
            end
            exprflag[] = true
            return extract_macro_flags(MacroTools.unblock(ex.args[3]), exprflags)
        else
            return (ex, exprflags)
        end
    end
    return (ex, exprflags)
end

"""
    check_macro_flags_consistency(exprflags)

Check that the macro flags are consistent with
one another. For now this only checks that
`@passmissing` is only called when `@byrow` is
also called. In the future we may expand
this function or eliminate it all together.
"""
function check_macro_flags_consistency(exprflags)
    if exprflags[PASSMISSING_SYM][]
        if !exprflags[BYROW_SYM][]
            s = "The `@passmissing` flag is currently only allowed with the `@byrow` flag"
            throw(ArgumentError(s))
        elseif exprflags[ASTABLE_SYM][]
            s = "The `@passmissing` flag is currently not allowed with the `@astable` flag"
            throw(ArgumentError(s))
        end
    end
end

"""
    get_source_fun(function_expr; wrap_byrow::Bool=false)

Given an expression that may contain `QuoteNode`s (`:x`)
and items wrapped in `cols`, return a function
that is equivalent to that expression where the
`QuoteNode`s and `cols` items are the inputs
to the function.

For fast compilation `get_source_fun` returns
the name of a called function where possible.

* `f(:x, :y)` will return `f`
* `f.(:x, :y)` will return `ByRow(f)`
* `:x .+ :y` will return `.+`

`get_source_fun` also returns an expression
representing the vector of inputs that will be
used as the `src` in the `src => fun => dest`
call later on.

If `wrap_byrow=true` then the function gets wrapped
in `ByRow`. If the expression begins with `@byrow`,
then `get_source_fun` is recursively called on the
expression that `@byrow` acts on, with `wrap_byrow=true`.

### Examples

```julia-repl
julia> using MacroTools

julia> ex = :(:x + :y);

julia> DataFramesMeta.get_source_fun(ex)
(:(DataFramesMeta.make_source_concrete([:x, :y])), :+)

julia> ex = quote
           :x .+ 1 .* :y
       end |> MacroTools.prettify

julia> src, fun = DataFramesMeta.get_source_fun(ex);

julia> MacroTools.prettify(fun)
:((mammoth, goat)->mammoth .+ 1 .* goat)

julia> ex = :(@byrow :x * :y);

julia> src, fun = DataFramesMeta.get_source_fun(ex);

julia> MacroTools.prettify(fun)
:(ByRow(*))
```
"""
function get_source_fun(function_expr; exprflags = deepcopy(DEFAULT_FLAGS))
    function_expr = MacroTools.unblock(function_expr)

    if is_simple_non_broadcast_call(function_expr)
        source = args_to_selectors(function_expr.args[2:end])
        fun_t = function_expr.args[1]

        # .+ to Expr(:., +)
        fun = fix_simple_dot(fun_t)
    elseif is_simple_broadcast_call(function_expr)
        # extract source symbols from quotenodes
        source = args_to_selectors(function_expr.args[2].args)
        fun_t = function_expr.args[1]
        fun = Expr(:., fun_t)
    elseif is_nested_fun_recursive(function_expr, false)
        composed_expr = make_composed(function_expr)
        # Repeat clean up from simple non-broadcast above
        source = args_to_selectors(composed_expr.args[2:end])
        fun = composed_expr.args[1]
    else
        membernames = Dict{Any, Symbol}()

        body = replace_syms!(membernames, function_expr)
        source = :($make_source_concrete($(Expr(:vect, keys(membernames)...))))
        inputargs = Expr(:tuple, values(membernames)...)
        fun = quote
            $inputargs -> begin
                $body
            end
        end
    end

    if exprflags[BYROW_SYM][]
        if exprflags[PASSMISSING_SYM][]
            fun = :($ByRow($(Missings.passmissing)($fun)))
        else
            fun = :($ByRow($fun))
        end
    end

    return source, fun
end

# `nolhs` needs to be `true` when we have syntax of the form
# `@combine(gd, fun(:x, :y))` where `fun` returns a `table` object.
# We don't create the "new name" pair because new names are
# given by the table.
# We need wrap_byrow as a keyword argument here in case someone
# uses `@transform df @byrow begin ... end`, which we
# deal with outside of this function.
function fun_to_vec(ex::Expr;
                    gensym_names::Bool=false,
                    outer_flags::NamedTuple=deepcopy(DEFAULT_FLAGS),
                    no_dest::Bool=false,
                    allow_multicol::Bool=false)
    # classify the type of expression
    # :x # handled via dispatch
    # $:x # handled as though above
    # All(), Between(...), Cols(...), Not(...), requires allow_multicol (only true in select)
    # f(:x) # requires no_dest, for `@with` and `@subset` in future
    # :y = :x # Simple pair
    # :y = $:x # Extract and return simple pair (no function)
    # $:y = :x # Simple pair
    # $:y = $:x # Simple pair
    # :y = f(:x) # re-write as simple call
    # :y = f($:x) # re-write as simple call, interpolation elsewhere
    # :y = :x + 1 # re-write as complicated call
    # :y = $:x + 1 # re-write as complicated call, interpolation elsewhere
    # $:y = f(:x) # re-write as simple call, unblock extract function
    # $:y = f($:x) # re-write as simple call, unblock, interpolation elsewhere
    # $y = :x + 1 # re-write as complicated col, unblock
    # $:y = $:x + 1 # re-write as complicated call, unblock, interpolation elsewhere
    # `@byrow` before any of the above
    ex, final_flags = extract_macro_flags(MacroTools.unblock(ex), deepcopy(outer_flags))
    check_macro_flags_consistency(final_flags)

    if gensym_names
        ex = Expr(:kw, QuoteNode(gensym()), ex)
    end

    # :x
    # handled below via dispatch on ::QuoteNode

    ex_col = get_column_expr(ex; allow_multicol = allow_multicol)
    if ex_col !== nothing
        return ex_col
    end

    if final_flags[ASTABLE_SYM][]
        src, fun = get_source_fun_astable(ex; exprflags = final_flags)

        return :($src => $fun => $AsTable)
    end

    if no_dest # subset and with
        src, fun = get_source_fun(ex, exprflags = final_flags)
        return quote
            $src => $fun
        end
    end

    if !(ex.head == :kw || ex.head == :(=))
        throw(ArgumentError("Malformed expression in DataFramesMeta.jl macro"))
    end

    lhs = let t = ex.args[1]
        if t isa Symbol
            t = QuoteNode(t)
            msg = "Using an un-quoted Symbol on the LHS is deprecated. " *
                  "Write $t = ... instead."

            @warn msg
        end

        s = get_column_expr(t)
        if s === nothing
            throw(ArgumentError("Malformed expression on LHS in DataFramesMeta.jl macro"))
        end

        s
    end

    rhs = MacroTools.unblock(ex.args[2])
    rhs_col = get_column_expr(rhs)
    if rhs_col !== nothing
        src = rhs_col
        dest = lhs
        return :($src => $dest)
    end

    if is_macro_head(rhs, "@byrow") || is_macro_head(rhs, "@passmissing")
        s = "In keyword argument inputs, `@byrow` and `@passmissing`" *
            "must be on the left hand side. " *
            "Did you write `y = @byrow f(:x)` instead of `@byrow y = f(:x)`?"
        throw(ArgumentError(s))
    end

    dest = lhs
    src, fun = get_source_fun(rhs; exprflags = final_flags)
    return :($src => $fun => $dest)
end

fun_to_vec(ex::QuoteNode;
           no_dest::Bool=false,
           gensym_names::Bool=false,
           outer_flags::Union{NamedTuple, Nothing}=nothing,
           allow_multicol::Bool = false) = ex


"""
    rename_kw_to_pair(ex::Expr)

Given an expression where the left- and right- hand side
both are both valid column identifiers,  i.e., a `QuoteNode`
or an expression beginning with `$DOLLAR`, or a "full" expression of the form
`$DOLLAR(:x => :y)`, return an expression, where expression arguments of type
`QuoteNode`` are converted to `String``.
"""
function rename_kw_to_pair(ex::Expr)
    ex_col = get_column_expr(ex)
    if ex_col !== nothing
        return ex_col
    end

    lhs = let t = ex.args[1]
        s = get_column_expr_rename(t)
        if s === nothing
            throw(ArgumentError("Invalid column identifier on LHS in @rename macro"))
        end
        s
    end

    rhs = let t = ex.args[2]
        s = get_column_expr_rename(t)
        if s === nothing
            throw(ArgumentError("Invalid column identifier on RHS in @rename macro"))
        end
        s
    end

    newname = lhs
    oldname = rhs
    return :($oldname => $newname)
end

function pairs_to_str_pairs(args...)
    map(args) do arg
        if !(arg isa Pair)
            throw(ArgumentError("Non-pair created in @rename"))
        end

        oldname = first(arg)
        newname = last(arg)

        if !(oldname isa Symbol || oldname isa AbstractString || oldname isa Integer)
            throw(ArgumentError("RHS in @rename must be an Integer, Symbol, or AbstractString"))
        end

        if !(newname isa Symbol || newname isa AbstractString)
            throw(ArgumentError("LHS in @rename must be a Symbol, or AbstractString"))
        end

        if oldname isa Integer
            return oldname => string(newname)
        end

        return string(oldname) => string(newname)
    end
end

function make_source_concrete(x::AbstractVector)
    if length(x) == 1 && x[1] isa AsTable
        return x[1]
    elseif isempty(x) || isconcretetype(eltype(x))
        return x
    elseif all(t -> t isa Union{AbstractString, Symbol}, x)
        return Symbol.(x)
    else
        throw(ArgumentError("Column references must be either all the same " *
                            "type or a a combination of `Symbol`s and strings"))
    end
end


function get_df_args_kwargs(x, args...; wrap_byrow = false)
    kw = []
    # x is normally a data frame. But if the call looks like
    # transform(df, :x = 1; copycols = false)
    # then x is actually Expr(:parameters, Expr(:kw, :copycole, false))
    # When this happens, we assign x to the data frame, use only
    # the rest of the args, and keep trask of the keyword argument.
    if x isa Expr && x.head === :parameters
        append!(kw, x.args)
        x = first(args)
        args = args[2:end]
    end

    if args isa Tuple
        blockarg = Expr(:block, args...)
    else
        blockarg = args
    end

    # create_args_vector! has an exclamation point because
    # we modify the keyword arguments kw
    transforms, outer_flags, kw = create_args_vector!(kw, blockarg; wrap_byrow = wrap_byrow)

    return (x, transforms, outer_flags, kw)
end

function get_kw_from_macro_call(e::Expr)
    if length(e.args) != 3
        throw(ArgumentError("Invalid @kwarg expression"))
    end

    nv = e.args[3]

    return nv
end

"""
   create_args_vector(arg) -> vec, outer_flags

Given an expression return a vector of operations
and a `NamedTuple` of the macro-flags that appear
in the expression.

If a `:block` expression, return the `args` of
the block as an array. If a simple expression,
wrap the expression in a one-element vector.
"""
function create_args_vector!(kw, arg; wrap_byrow::Bool=false)
    arg, outer_flags = extract_macro_flags(MacroTools.unblock(arg))

    if wrap_byrow
        if outer_flags[BYROW_SYM][]
            throw(ArgumentError("Redundant @byrow calls"))
        end

        outer_flags[BYROW_SYM][] = true
    end

    # @astable means the whole block is one transformation

    if arg isa Expr && arg.head == :block && !outer_flags[ASTABLE_SYM][]
        x = MacroTools.rmlines(arg).args
        transforms = []
        seen_kw_macro = false
        for xi in x
            if is_macro_head(xi, "@kwarg")
                kw_item = get_kw_from_macro_call(xi)
                push!(kw, kw_item)
                seen_kw_macro = true
            else
                if seen_kw_macro
                    throw(ArgumentError("@kwarg calls must be at end of block"))
                end
                push!(transforms, xi)
            end
        end
    else
        transforms = Any[arg]
    end

    return transforms, outer_flags, kw
end
