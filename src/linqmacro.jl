export @linq, linq

##############################################################################
##
## @linq - general macro that creates a mini DSL for chaining and macro and
##         function calls
##
##############################################################################
"""
```julia
@linq df ...
```
General macro that creates a mini DSL for chaining and macro calls.

### Details

The following embedded function calls are equivalent to their macro version:

- `with`
- `where`
- `select`
- `transform`
- `by`
- `groupby`
- `orderby`
- `based_on`

### Examples

```julia
n = 100
df = DataFrame(a = rand(1:3, n),
               b = ["a","b","c","d"][rand(1:4, n)],
               x = rand(n))

x1 = @linq transform(where(df, :a .> 2, :b .!= "c"), y = 10 * :x)
x1 = @linq by(x1, :b, meanX = mean(:x), meanY = mean(:y))
x1 = @linq select(orderby(x1, :b, -:meanX), var = :b, :meanX, :meanY)

xl = @linq df |>
    transform(y = 10 * :x) |>
    where(:a .> 2) |>
    by(:b, meanX = mean(:x), meanY = mean(:y)) |>
    orderby(:meanX) |>
    select(:meanX, :meanY, var = :b)
```

"""
macro linq(arg)
    esc(replacefuns(replacechains(arg)))
end

# Snippet from Calculus.jl
type SymbolParameter{T} end
SymbolParameter(s::Symbol) = SymbolParameter{s}()

replacefuns(x) = x  # default for non-expression stuff
function replacefuns(e::Expr)
    for i in 1:length(e.args)
        e.args[i] = replacefuns(e.args[i])
    end
    if e.head == :call
        return linq(SymbolParameter(e.args[1]), e.args[2:end]...)
    else
        return e
    end
end

replacechains(x) = x
function replacechains(e::Expr)
    for i in 1:length(e.args)
        e.args[i] = replacechains(e.args[i])
    end
    if e.head == :call && e.args[1] == :|> && isa(e.args[3], Expr)
        newe = e.args[3]
        insert!(newe.args, 2, e.args[2])
        return newe
    else
        return e
    end
end


##############################################################################
##
## Various linq helper definitions
##
##############################################################################

## Default, no-op:
linq{s}(::SymbolParameter{s}, args...) = Expr(:call, s, args...)

function linq(::SymbolParameter{:with}, d, body)
    with_helper(d, body)
end

function linq(::SymbolParameter{:ix}, d, args...)
    Base.depwarn("`ix` is deprecated; use `where` and `select`.", :ix)
    ix_helper(d, args...)
end

function linq(::SymbolParameter{:where}, d, args...)
    where_helper(d, args...)
end

function linq(::SymbolParameter{:orderby}, d, args...)
    :(let _D = $d;  orderby(_D, _DF -> @with(_DF, DataFramesMeta.orderbyconstructor(_D)($(args...)))); end)
end

function linq(::SymbolParameter{:transform}, x, args...)
    transform_helper(x, args...)
end

function linq(::SymbolParameter{:based_on}, x, args...)
    :( DataFrames.combine(map(_DF -> DataFramesMeta.@with(_DF, DataFrames.DataFrame($(args...))), $x)) )
end

function linq(::SymbolParameter{:by}, x, what, args...)
    :( by($x, $what, _DF -> @with(_DF, DataFrame($(args...)))) )
end

function linq(::SymbolParameter{:select}, x, args...)
    :(let _DF = $x; @with(_DF, select(_DF, $(expandargs(args)...))); end)
end
