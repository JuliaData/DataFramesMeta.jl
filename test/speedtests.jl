module SpeedTests

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta
using Devectorize

srand(1)
const n = 5_000_000
a = rand(n)
b = rand(n)
da = data(a)
db = data(b)
df = DataFrame(a = da, b = db)
df2 = DataFrame(Any[a, b])
names!(df2, [:a, :b])

Base.values(da::DataArray) = da.data

function dot1(a::Vector, b::Vector)
    x = 0.0
    for i in 1:length(a)
        x += a[i] * b[i]
    end
    return x
end

function dot2(da::DataVector, db::DataVector)
    T = eltype(da)
    x = 0.0
    for i in 1:length(da)
        x += da[i]::T * db[i]::T
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

function dot5(da::DataVector, db::DataVector)
    x = 0.0
    for i in 1:length(da)
        x += da.data[i] * db.data[i]
    end
    return x
end

function dot6(da::DataVector, db::DataVector)
    x = 0.0
    for i in 1:length(da)
        x += values(da)[i] * values(db)[i]
    end
    return x
end

function dot7(da::DataVector, db::DataVector)
    x = 0.0
    for i in 1:length(da)
        if !(isna(da, i) || isna(da, i))
            x += values(da)[i] * values(db)[i]
        end
    end
    return x
end

function dot8(a::Vector, b::Vector)
    x = 0.0
    for i in 1:length(a)
        if !(isnan(a[i]) || isnan(a[i]))
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

## function dot10(df::DataFrame)
##     @with df begin
##         @devec x = sum(:a .* :b)
##         x
##     end
## end
## t10 = @elapsed dot10(df)

t1 = @elapsed dot1(a, b)
t2 = @elapsed dot2(da, db)
t3 = @elapsed dot3(df)
t4 = @elapsed dot4(df)
t5 = @elapsed dot5(da, db)
t6 = @elapsed dot6(da, db)
t7 = @elapsed dot7(da, db)
t8 = @elapsed dot8(a, b)
t9 = @elapsed dot9(df)

end
