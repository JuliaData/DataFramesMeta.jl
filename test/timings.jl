
module DataFramesTimings

using Random
using DataFrames
using DataFramesMeta

Random.seed!(1)
const n = 5_000_000
a = rand(n)
b = rand(n)
df = DataFrame(a = da, b = db)

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
            x += (:a).data[i] * (:b).data[i]
        end
        x
    end
end

@show t3 = @elapsed dot3(df)
@show t4 = @elapsed dot4(df)
@show t5 = @elapsed dot5(df)

end # module
