
module TestLinqMacro

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta

srand(1)
n = 100
df = DataFrame(a = rand(1:3, n),
               b = ["a","b","c","d"][rand(1:4, n)],
               x = rand(n))

x = @where(df, :a .> 2, :b .!= "c")
x = @transform(x, y = 10 * :x)
x = @by(x, :b, meanX = mean(:x), meanY = mean(:y))
x = @orderby(x, :b, -:meanX)
x = @select(x, var = :b, :meanX, :meanY)

x1 = @linq transform(where(df, :a .> 2, :b .!= "c"), y = 10 * :x)
x1 = @linq by(x1, :b, meanX = mean(:x), meanY = mean(:y))
x1 = @linq select(orderby(x1, :b, -:meanX), var = :b, :meanX, :meanY)

## chaining
xlinq = @linq df  |>
    where(:a .> 2, :b .!= "c")  |>
    transform(y = 10 * :x)  |>
    by(:b, meanX = mean(:x), meanY = mean(:y))  |>
    orderby(:b, -:meanX)  |>
    select(var = :b, :meanX, :meanY)

@test x == x1
@test x == xlinq

end
