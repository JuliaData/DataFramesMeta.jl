
module DataFramesTimings

using Compat.Random
using DataArrays, DataFrames
using DataFramesMeta

srand(1)
const n = 5_000_000
a = rand(n)
b = rand(n)
da = data(a)
db = data(b)
df = DataFrame(a = da, b = db)

Base.values(da::DataArray) = da.data

function dot1(da::DataVector, db::DataVector)
    T = eltype(da)
    x = 0.0
    for i in 1:length(da)
        x += da[i]::T * db[i]::T
    end
    return x
end

function dot2(da::DataVector, db::DataVector)
    x = 0.0
    for i in 1:length(da)
        x += da.data[i] * db.data[i]
    end
    return x
end

function dot3(df::DataFrame)
    da, db = df[:a], df[:b]
    T = eltype(da)
    x = 0.0
    for i in 1:length(da)
        x += df[:a][i] * df[:a][i]
    end
    return x
end

function dot4(df::DataFrame)
    da, db = df[:a], df[:b]
    T = eltype(da)
    x = 0.0
    for i in 1:length(da)
        x += da[i]::T * db[i]::T
    end
    return x
end

function dot5(df::DataFrame)
    @with df begin
        x = 0.0
        for i in 1:length(:a)
            ## x += values(:a)[i] * values(:b)[i]
            x += (:a).data[i] * (:b).data[i]
        end
        x
    end
end

@show t1 = @elapsed dot1(da, db)
@show t2 = @elapsed dot2(da, db)
@show t3 = @elapsed dot3(df)
@show t4 = @elapsed dot4(df)
@show t5 = @elapsed dot5(df)

end # module
