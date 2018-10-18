module SpeedTests

using Test, Random
using DataFrames
using DataFramesMeta
using Devectorize

Random.seed!(1)
const n = 5_000_000
a = rand(n)
b = rand(n)
df = DataFrame(a = da, b = db)
df2 = DataFrame(Any[a, b])
names!(df2, [:a, :b])

function dot1(a::Vector, b::Vector)
    x = 0.0
    for i in 1:length(a)
        x += a[i] * b[i]
    end
    return x
end

function dot3(df::DataFrame)
    da, db = df[:a], df[:b]
    T = eltype(da)
    x = 0.0
    for i in 1:length(da)
        x += da[i]::T * db[i]::T
    end
    return x
end

function dot4(df::DataFrame)
    da, db = df[:a], df[:b]
    return dot2(da, db)
end

function dot8(a::Vector, b::Vector)
    x = 0.0
    for i in 1:length(a)
        if !(isnan(a[i]) || isnan(b[i]))
            x += a[i] * b[i]
        end
    end
    return x
end

function dot9(df::DataFrame)
    @with df begin
        x = 0.0
        for i in 1:length(:a)
            x += values(:a)[i] * values(:b)[i]
        end
        x
    end
end

t1 = @elapsed dot1(a, b)
t3 = @elapsed dot3(df)
t4 = @elapsed dot4(df)
t8 = @elapsed dot8(a, b)
t9 = @elapsed dot9(df)

end
