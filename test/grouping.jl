

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

# Transform tests
d = DataFrame(a = [1,1,1,2,2,3,3,1], 
              b = Any[1,2,3,missing,missing,6.0,5.0,4], 
              c = CategoricalArray([1,2,3,1,2,3,1,2]))
g = groupby(d, :a)
## Scalar output 
# Type promotion Int -> Float
@test @transform(g, t = :b[1])[1, :t] == 1.0
# Type promotion Number -> Any
@test @transform(g, t = isequal(:b[1], 1) ? :b[1] : "a")[1, :t] == 1
## Vector output 
# Normal use
@test @transform(g, t = :b .- mean(:b))[:t][1, :t] == -1.5
# Type promotion
@test @transform(g, t = isequal(:b[1], 1) ? fill(1, length(:b)) : fill(2.0, length(:b)))[1, :t] == 1.0
# Vectors of different types
@test @transform(g, t = isequal(:b[1], 1) ? :b : fill("a", length(:b)))[1, :t] == 1
# Categorical Categorical Array 

end # module
