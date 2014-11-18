
module CompositeDataFramesTimings

using DataArrays, DataFrames
using DataFramesMeta

srand(1)
const n = 5_000_000
a = rand(n)
b = rand(n)
cdf = CompositeDataFrame(a = a, b = b)
df = DataFrame(cdf)

function dot1(df::AbstractDataFrame)
    x = 0.0
    for i in 1:size(df, 1)
        x += df[:a][i] * df[:a][i]
    end
    return x
end

function dot2(df::AbstractDataFrame)
    x = 0.0
    for i in 1:size(df, 1)
        x += df[i,:a] * df[i,:a]
    end
    return x
end

function dot3(df::AbstractDataFrame)
    @with df begin
        x = 0.0
        for i in 1:length(:a)
            x += (:a)[i] * (:b)[i]
        end
        x
    end
end

@show t1 = @elapsed dot1(df)
@show t2 = @elapsed dot2(df)
@show t3 = @elapsed dot3(df)
@show t1c = @elapsed dot1(cdf)
@show t2c = @elapsed dot2(cdf)
@show t3c = @elapsed dot3(cdf)

end # module
