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
get_column_expr(x) = nothing
function get_column_expr(e::Expr)
    e.head == :$ && return e.args[1]
    onearg(e, :AsTable) && return e
    if onearg(e, :cols)
        Base.depwarn("cols is deprecated use $DOLLAR to escape column names instead", :cols)
        return e.args[2]
    end
    return nothing
end
get_column_expr(x::QuoteNode) = x

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

is_simple_non_broadcast_call(x) = false
function is_simple_non_broadcast_call(expr::Expr)
    expr.head == :call &&
        length(expr.args) >= 2 &&
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

    :(DataFramesMeta.make_source_concrete($(Expr(:vect, t...))))
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
then `get_source_fun` is recurively called on the
expression that `@byrow` acts on, with `wrap_byrow=true`.

### Examples

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

        # .+ to +
        if startswith(string(fun_t), '.')
            f_sym_without_dot = Symbol(chop(string(fun_t), head = 1, tail = 0))
            fun = :(DataFrames.ByRow($f_sym_without_dot))
        else
            fun = fun_t
        end
    elseif is_simple_broadcast_call(function_expr)
        # extract source symbols from quotenodes
        source = args_to_selectors(function_expr.args[2].args)
        fun_t = function_expr.args[1]
        fun = :(DataFrames.ByRow($fun_t))
    else
        membernames = Dict{Any, Symbol}()

        body = replace_syms!(membernames, function_expr)
        source = :(DataFramesMeta.make_source_concrete($(Expr(:vect, keys(membernames)...))))
        inputargs = Expr(:tuple, values(membernames)...)
        fun = quote
            $inputargs -> begin
                $body
            end
        end
    end

    if exprflags[BYROW_SYM][]
        if exprflags[PASSMISSING_SYM][]
            fun = :(ByRow(Missings.passmissing($fun)))
        else
            fun = :(ByRow($fun))
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
                    no_dest::Bool=false)
    # classify the type of expression
    # :x # handled via dispatch
    # $:x # handled as though above
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

    ex_col = get_column_expr(ex)
    if ex_col !== nothing
        return ex_col
    end

    if final_flags[ASTABLE_SYM][]
        src, fun = get_source_fun_astable(ex; exprflags = final_flags)

        return :($src => $fun => AsTable)
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
           outer_flags::Union{NamedTuple, Nothing}=nothing) = ex

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

function create_args_vector(args...; wrap_byrow::Bool=false)
    create_args_vector(Expr(:block, args...); wrap_byrow = wrap_byrow)
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
function create_args_vector(arg; wrap_byrow::Bool=false)
    arg, outer_flags = extract_macro_flags(MacroTools.unblock(arg))

    if wrap_byrow
        if outer_flags[BYROW_SYM][]
            throw(ArgumentError("Redundant @byrow calls"))
        end

        outer_flags[BYROW_SYM][] = true
    end

    if arg isa Expr && arg.head == :block && !outer_flags[ASTABLE_SYM][]
        x = MacroTools.rmlines(arg).args
    else
        x = Any[arg]
    end

    return x, outer_flags
end
