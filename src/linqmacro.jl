export @linq, linq

##############################################################################
##
## @linq - general macro that creates a mini DSL for chaining and macro and
##         function calls
##
##############################################################################
"""
    @linq df ...

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

```jldoctest
julia> using DataFrames, DataFramesMeta, Statistics

julia> n = 100;

julia> df = DataFrame(a = rand(1:3, n),
                      b = ["a","b","c","d"][rand(1:4, n)],
                      x = rand(n));

julia> x1 = @linq transform(where(df, :a .> 2, :b .!= "c"), y = 10 * :x);

julia> x1 = @linq by(x1, :b, meanX = mean(:x), meanY = mean(:y));

julia> @linq select(orderby(x1, :b, -:meanX), var = :b, :meanX, :meanY)
3×3 DataFrames.DataFrame
│ Row │ var │ meanX    │ meanY   │
├─────┼─────┼──────────┼─────────┤
│ 1   │ "a" │ 0.665682 │ 6.65682 │
│ 2   │ "b" │ 0.617848 │ 6.17848 │
│ 3   │ "d" │ 0.568289 │ 5.68289 │

julia> @linq df |>
           transform(y = 10 * :x) |>
           where(:a .> 2) |>
           by(:b, meanX = mean(:x), meanY = mean(:y)) |>
           orderby(:meanX) |>
           select(:meanX, :meanY, var = :b)
4×3 DataFrames.DataFrame
│ Row │ meanX    │ meanY   │ var │
├─────┼──────────┼─────────┼─────┤
│ 1   │ 0.353205 │ 3.53205 │ "a" │
│ 2   │ 0.419833 │ 4.19833 │ "d" │
│ 3   │ 0.452061 │ 4.52061 │ "c" │
│ 4   │ 0.519316 │ 5.19316 │ "b" │
```

"""
macro linq(arg)
    esc(replacefuns(replacechains(arg)))
end

# Snippet from Calculus.jl
struct SymbolParameter{T} end
SymbolParameter(s::Symbol) = SymbolParameter{s}()

replacefuns(x) = x  # default for non-expression stuff
function replacefuns(e::Expr)
    for i in 1:length(e.args)
        e.args[i] = replacefuns(e.args[i])
    end
    if e.head == :call && isa(e.args[1], Symbol)
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
linq(::SymbolParameter{s}, args...) where {s} = Expr(:call, s, args...)

function linq(::SymbolParameter{:with}, d, body)
    with_helper(d, body)
end

function linq(::SymbolParameter{:where}, d, args...)
    where_helper(d, args...)
end

function linq(::SymbolParameter{:orderby}, d, args...)
    orderby_helper(d, args...)
end

function linq(::SymbolParameter{:transform}, x, args...)
    transform_helper(x, args...)
end

function linq(::SymbolParameter{:based_on}, x, args...)
    based_on_helper(x, args...)
end

function linq(::SymbolParameter{:by}, x, what, args...)
    by_helper(x, what, args...)
end

function linq(::SymbolParameter{:select}, x, args...)
    select_helper(x, args...)
end
