function addkey!(membernames, nam)
    if !haskey(membernames, nam)
        membernames[nam] = gensym()
    end
    membernames[nam]
end

onearg(e::Expr, f) = e.head == :call && length(e.args) == 2 && e.args[1] == f
onearg(e, f) = false

mapexpr(f, e) = Expr(e.head, map(f, e.args)...)

replace_syms!(x, membernames) = x
replace_syms!(q::QuoteNode, membernames) =
    replace_syms!(Meta.quot(q.value), membernames)
replace_syms!(e::Expr, membernames) =
    if onearg(e, :^)
        e.args[2]
    elseif onearg(e, :_I_)
        @warn "_I_() for escaping variables is deprecated, use cols() instead"
        addkey!(membernames, :($(e.args[2])))
    elseif onearg(e, :cols)
        addkey!(membernames, :($(e.args[2])))
    elseif e.head == :quote
        addkey!(membernames, Meta.quot(e.args[1]) )
    elseif e.head == :.
        replace_dotted!(e, membernames)
    else
        mapexpr(x -> replace_syms!(x, membernames), e)
    end

is_simple_non_broadcast_call(x) = false
function is_simple_non_broadcast_call(expr::Expr)
    expr.head == :call &&
        length(expr.args) >= 2 &&
        expr.args[1] isa Symbol &&
        all(x -> x isa QuoteNode || onearg(x, :cols), expr.args[2:end])
end

is_simple_broadcast_call(x) = false
function is_simple_broadcast_call(expr::Expr)
    expr.head == :. &&
        length(expr.args) == 2 &&
        expr.args[1] isa Symbol &&
        expr.args[2] isa Expr &&
        expr.args[2].head == :tuple &&
        all(x -> x isa QuoteNode || onearg(x, :cols), expr.args[2].args)
end

function args_to_selectors(v)
    t = map(v) do arg
        if arg isa QuoteNode
            arg
        elseif onearg(arg, :cols)
            arg.args[2]
        else
            throw(ArgumentError("This path should not be reached, arg: $(arg)"))
        end
    end

    :(DataFramesMeta.make_source_concrete($(Expr(:vect, t...))))
end

is_macro_head(ex, name) = false
is_macro_head(ex::Expr, name) = ex.head == :macrocall && ex.args[1] == Symbol(name)

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
function get_source_fun(function_expr; wrap_byrow::Bool=false)
    println(Main.MacroTools.prettify(function_expr))
    # recursive step for begin :a + :b end
    if is_macro_head(function_expr, "@byrow")
        if wrap_byrow
            throw(ArgumentError("Redundant `@byrow` calls."))
        end
        return get_source_fun(function_expr.args[3], wrap_byrow=true)
    elseif function_expr isa Expr &&
        function_expr.head == :block &&
        length(function_expr.args) == 1

        return get_source_fun(function_expr.args[1])
    elseif is_simple_non_broadcast_call(function_expr)
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

        body = replace_syms!(function_expr, membernames)

        source = :(DataFramesMeta.make_source_concrete($(Expr(:vect, keys(membernames)...))))
        inputargs = Expr(:tuple, values(membernames)...)

        fun = quote
            $inputargs -> begin
                $body
            end
        end
    end

    if wrap_byrow
        fun = :(ByRow($fun))
    end

    return source, fun
end

# `nolhs` needs to be `true` when we have syntax of the form
# `@combine(gd, fun(:x, :y))` where `fun` returns a `table` object.
# We don't create the "new name" pair because new names are
# given by the table.
function fun_to_vec(ex::Expr; nolhs::Bool = false, gensym_names::Bool = false, wrap_byrow::Bool=false)
    # classify the type of expression
    # :x # handled via dispatch
    # cols(:x) # handled as though above
    # y = :x # :x is a QuoteNode
    # y = cols(:x) # use cols on RHS
    # cols(:y) = :x # RHS in :block
    # cols(:y) = cols(:x) #
    # y = f(:x) # re-write as simple call
    # y = f(cols(:x)) # re-write as simple call, use cols
    # y = :x + 1 # re-write as complicated call
    # y = cols(:x) + 1 # re-write as complicated call, with cols
    # cols(:y) = f(:x) # re-write as simple call, but RHS is :block
    # cols(:y) = f(cols(:x)) # re-write as simple call, RHS is block, use cols
    # cols(y) = :x + 1 # re-write as complicated col, but RHS is :block
    # cols(:y) = cols(:x) + 1 # re-write as complicated call, RHS is block, use cols
    # `@byrow` before any of the above
    if is_macro_head(ex, "@byrow")
        if wrap_byrow
            throw(ArgumentError("Redundant `@byrow` call."))
        end
        return fun_to_vec(ex.args[3]; gensym_names = gensym_names, wrap_byrow = true)
    end

    if gensym_names
        ex = Expr(:kw, gensym(), ex)
    end

    # nokw = (ex.head !== :(=)) && (ex.head !== :kw) && nolhs

    # :x
    # handled below via dispatch on ::QuoteNode

    # cols(:x)
    if onearg(ex, :cols)
        return ex.args[2]
    end

    # The above cases are the only ones allowed
    # if you don't have nolhs explicitely stated
    # or are just `:x` or `cols(x)`
    # if !(ex.head === :(=) || ex.head === :kw || nolhs)
    #     throw(ArgumentError("Expressions not of the form `y = f(:x)` are currently disallowed."))
    # end

    # f(:x) # it's assumed this returns a Table
    # (; a = :x, ) # something more explicit we might see
    if is_macro_head(ex, "@astable")

        source, fun = get_source_fun(ex.args[3])
        return quote
            $source => $fun => AsTable
        end
    end


    lhs = ex.args[1]
    rhs_t = ex.args[2]
    # if lhs is a cols(y) then the rhs gets parsed as a block
    if onearg(lhs, :cols) && rhs_t.head === :block && length(rhs_t.args) == 1
        rhs = rhs_t.args[1]
    else
        rhs = rhs_t
    end

    if is_macro_head(rhs, "@byrow")
        s = "In keyword argument inputs, `@byrow` must be on the left hand side. " *
        "Did you write `y = @byrow f(:x)` instead of `@byrow y = f(:x)`?"
        throw(ArgumentError(s))
    end

    # y = :x
    if lhs isa Symbol && rhs isa QuoteNode
        source = rhs
        dest = QuoteNode(lhs)

        return quote
            $source => $dest
        end
    end

    # y = cols(:x)
    if lhs isa Symbol && onearg(rhs, :cols)
        source = rhs.args[2]
        dest = QuoteNode(lhs)

        return quote
            $source => $dest
        end
    end

    # cols(:y) = :x
    if onearg(lhs, :cols) && rhs isa QuoteNode
        source = rhs
        dest = lhs.args[2]

        return quote
            $source => $dest
        end
    end

    # cols(:y) = cols(:x)
    if onearg(lhs, :cols) && onearg(rhs, :cols)
        source = rhs.args[2]
        dest = lhs.args[2]

        return quote
            $source => $dest
        end
    end

    # y = f(:x)
    # y = f(cols(:x))
    # y = :x + 1
    # y = cols(:x) + 1
    source, fun = get_source_fun(rhs; wrap_byrow = wrap_byrow)
    if lhs isa Symbol
        dest = QuoteNode(lhs)

        return quote
            $source => $fun => $dest
        end
    end

    # cols(:y) = f(:x)
    if onearg(lhs, :cols)
        dest = lhs.args[2]

        return quote
            $source => $fun => $dest
        end
    end

    throw(ArgumentError("This path should not be reached"))
end
fun_to_vec(ex::QuoteNode; gensym_names::Bool = false, wrap_byrow::Bool = false) = ex

function make_source_concrete(x::AbstractVector)
    if isempty(x) || isconcretetype(eltype(x))
        return x
    elseif all(t -> t isa Union{AbstractString, Symbol}, x)
        return Symbol.(x)
    else
        throw(ArgumentError("Column references must be either all the same " *
                            "type or a a combination of `Symbol`s and strings"))
    end
end

protect_replace_syms!(e, membernames) = e
function protect_replace_syms!(e::Expr, membernames)
    if e.head == :quote
        e
    else
        replace_syms!(e, membernames)
    end
end

function replace_dotted!(e, membernames)
    x_new = replace_syms!(e.args[1], membernames)
    y_new = protect_replace_syms!(e.args[2], membernames)
    Expr(:., x_new, y_new)
end

"""
    create_args_vector(args...) -> vec, wrap_byrow

Given multiple arguments which can be any type
of expression-like object (`Expr`, `QuoteNode`, etc.),
puts them into a single array, removing line numbers.
"""
function create_args_vector(args...)
    Any[Base.remove_linenums!(arg) for arg in args], false
end

"""
   create_args_vector(arg) -> vec, wrap_byrow

Normalize a single input to a vector of expressions,
with a `wrap_byrow` flag indicating that the
expressions should operate by row.

If `arg` is a single `:block`, it is unnested.
Otherwise, return a single-element array.
Also removes line numbers.

If `arg` is of the form `@byrow ...`, then
`wrap_byrow` is returned as `true`.
"""
function create_args_vector(arg)
    if arg isa Expr && is_macro_head(arg, "@byrow")
        wrap_byrow = true
        largs = length(arg.args)
        if largs == 2
            throw(ArgumentError("No transformations supplied with `@byrow`"))
        elseif largs == 3
            arg = arg.args[3]
        else
            arg = Expr(:block, arg.args[3:end]...)
        end
    else
        wrap_byrow = false
    end

    if arg isa Expr && arg.head == :block
        x = Base.remove_linenums!(arg).args
    else
        x = Any[Base.remove_linenums!(arg)]
    end

    if wrap_byrow && any(t -> is_macro_head(t, "@byrow"), x)
        throw(ArgumentError("Redundant `@byrow` calls."))
    end
    return x, wrap_byrow
end
