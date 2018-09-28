

module TestGrouping

using Test
using DataFrames
using DataFramesMeta
using Statistics
using Tables

d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x, sort=true)

@test  @where(d, :x .== 3) == DataFramesMeta.where(d, x -> x[:x] .== 3)
@test  DataFrame(@where(g, length(:x) > 5)) == DataFrame(DataFramesMeta.where(g, x -> length(x[:x]) > 5))
@test  DataFrame(@where(g, length(:x) > 5))[:n][1:3] == [5, 6, 7]

@test  DataFrame(DataFramesMeta.orderby(g, x -> mean(x[:n]))) == DataFrame(@orderby(g, mean(:n)))

@test  (@transform(g, y = :n .- median(:n)))[1,:y] == -5.0

d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x, sort=true)
@test @based_on(g, nsum = sum(:n))[:nsum] == [99, 84, 27]

d = DataFrame(a = [1,1,1,2,2], b = [1,2,3,missing,missing])
g = groupby(d, :a)
@test isequal(@transform(g, t = mean(:b))[:t], [2.0, 2.0, 2.0, missing, missing])
@test isequal(@transform(g, t = fill(:b[1], length(:b))), [1, 1, 1, missing, missing])

end # module
