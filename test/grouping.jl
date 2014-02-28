

module TestGrouping

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta

srand(1)
d = DataFrame(n = 1:20, x = rand(1:3, 20))
g = groupby(d, :x)

@test  @where(d, :x .== 3) == where(d, x -> x[:x] .== 3)
@test  DataFrame(@where(g, length(:x) > 5)) == DataFrame(where(g, x -> length(x[:x]) > 5))
@test  DataFrame(@where(g, length(:x) > 5))[:n][1:9] == @data [3, 5, 12, 14, 17, 19, 1, 2, 4]

@test  DataFrame(orderby(g, x -> mean(x[:n]))) == DataFrame(@orderby(g, mean(:n)))
@test  DataFrames.based_on(@orderby(g, mean(:n)), x ) == [2,1,3]




end # module
