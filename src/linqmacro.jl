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
- `combine`

### Examples

```jldoctest
julia> using DataFramesMeta, Statistics

julia> df = DataFrame(
            a = repeat(1:4, outer = 2),
            b = repeat(2:-1:1, outer = 4),
            x = 1:8);

julia> x1 = @linq transform(where(df, :a .> 2, :b .!= "c"), y = 10 .* :x);

julia> x1 = @linq by(x1, :b, meanX = mean(:x), meanY = mean(:y));

julia> @linq select(orderby(x1, :b, -:meanX), var = :b, :meanX, :meanY)
2×3 DataFrame
│ Row │ var   │ meanX   │ meanY   │
│     │ Int64 │ Float64 │ Float64 │
├─────┼───────┼─────────┼─────────┤
│ 1   │ 1     │ 6.0     │ 60.0    │
│ 2   │ 2     │ 5.0     │ 50.0    │

julia> @linq df |>
           transform(y = 10 .* :x) |>
           where(:a .> 2) |>
           by(:b, meanX = mean(:x), meanY = mean(:y)) |>
           orderby(:meanX) |>
           select(:meanX, :meanY, var = :b)
2×3 DataFrame
│ Row │ meanX   │ meanY   │ var   │
│     │ Float64 │ Float64 │ Int64 │
├─────┼─────────┼─────────┼───────┤
│ 1   │ 5.0     │ 50.0    │ 2     │
│ 2   │ 6.0     │ 60.0    │ 1     │

```
"""
macro linq(arg)

    @warn "@linq is deprecated. Use @chain instead. See ?@chain for details"

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

function linq(::SymbolParameter{:combine}, x, args...)
    combine_helper(x, args...)
end

function linq(::SymbolParameter{:by}, x, what, args...)
    by_helper(x, what, args...)
end

function linq(::SymbolParameter{:select}, x, args...)
    select_helper(x, args...)
end
