module TestChaining

using Test, Random
using DataFrames
using DataFramesMeta
using Lazy
using Statistics
using Random

Random.seed!(1)
n = 100
df = DataFrame(a = rand(1:3, n),
               b = ["a","b","c","d"][rand(1:4, n)],
               x = rand(n))

x = @where(df, :a .> 2)
x = @transform(x, y = 10 * :x)
x = @by(x, :b, meanX = mean(:x), meanY = mean(:y))
x = @orderby(x, :b, -:meanX)
x = @select(x, var = :b, :meanX, :meanY)

x_as = @as _x_ begin
    df
    @where(_x_, :a .> 2)
    @transform(_x_, y = 10 * :x)
    @by(_x_, :b, meanX = mean(:x), meanY = mean(:y))
    @orderby(_x_, :b, -:meanX)
    @select(_x_, var = :b, :meanX, :meanY)
end

# Uncomment and add to README.md when it starts working:
# @> is broken in 0.7 Lazy
#x_thread = @> begin
#    df
#    @where(:a .> 2)
#    @transform(y = 10 * :x)
#    @by(:b, meanX = mean(:x), meanY = mean(:y))
#    @orderby(:b, -:meanX)
#    @select(var = :b, :meanX, :meanY)
#end

@test x == x_as
#@test x == x_thread


end # module
