

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
g = groupby(d, :a, sort = false)
## Scalar output 
# Type promotion Int -> Float
t = @transform(g, t = :b[1])[:t] 
correct = [1.0, 1.0, 1.0, 1.0, missing, missing, 6.0, 6.0]
@test all(t .=== correct) && typeof(t) == typeof(correct)
# Type promotion Number -> Any
t = @transform(g, t = isequal(:b[1], 1) ? :b[1] : "a")[:t]
correct = Any[1, 1, 1, 1,"a" ,"a" ,"a" ,"a"]
@test all(t .=== correct) && typeof(t) == typeof(correct)
## Vector output 
# Normal use
t = @transform(g, t = :b .- mean(:b))[:t]
correct = Union{Float64, Missing}[-1.5, -0.5, 0.5, 1.5, missing, missing, 0.5, -0.5]
@test all(t .=== correct) && typeof(t) == typeof(correct)
# Type promotion
t = @transform(g, t = isequal(:b[1], 1) ? fill(1, length(:b)) : fill(2.0, length(:b)))[:t] 
correct = Float64[1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0]
@test all(t .=== correct) && typeof(t) == typeof(correct)
# Vectors of different types
t = @transform(g, t = isequal(:b[1], 1) ? :b : fill("a", length(:b)))[:t]
correct = Any[1, 2, 3, 4, "a", "a", "a", "a"]
@test all(t .=== correct) && typeof(t) == typeof(correct)
# Categorical Categorical Array 
# Scalar
t = @transform(g, t = :c[1])[:t]
correct = CategoricalArray([1, 1, 1, 1, 1, 1, 3, 3])
@test all(isequal.(t, correct)) && typeof(t) == typeof(correct)
# Vector 
t = @transform(g, t = :c)[:t]
correct = CategoricalArray([1, 2, 3, 2, 1, 2, 3, 1])
@test all(isequal.(t, correct)) && typeof(t) == typeof(correct)
end # module
