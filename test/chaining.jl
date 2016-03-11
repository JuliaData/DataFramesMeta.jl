module TestChaining

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta
using Lazy

srand(1)
n = 100
df = DataFrame(a = rand(1:3, n),
               b = ["a","b","c","d"][rand(1:4, n)],
               x = rand(n))

x = @where(df, :a .> 2)
x = @transform(x, y = 10 * :x)
x = @by(x, :b, meanX = mean(:x), meanY = mean(:y))
x = @orderby(x, :b, -:meanX)
x = @select(x, var = :b, :meanX, :meanY)

x_as = @as _ begin
    df
    @where(_, :a .> 2)
    @transform(_, y = 10 * :x)
    @by(_, :b, meanX = mean(:x), meanY = mean(:y))
    @orderby(_, :b, -:meanX)
    @select(_, var = :b, :meanX, :meanY)
end

x_thread = @> begin
    df
    @where(:a .> 2)
    @transform(y = 10 * :x)
    @by(:b, meanX = mean(:x), meanY = mean(:y))
    @orderby(:b, -:meanX)
    @select(var = :b, :meanX, :meanY)
end

@test x == x_as
@test x == x_thread


end # module
