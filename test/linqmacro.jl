module TestLinqMacro

using Test
using DataFrames
using DataFramesMeta
using Statistics
using Random

Random.seed!(100)
n = 100
df = DataFrame(a = rand(1:3, n),
               b = ["a","b","c","d"][rand(1:4, n)],
               x = rand(n))

x = @where(df, :a .> 2, :b .!= "c")
x = @transform(x, y = 10 * :x)
x = @by(x, :b, meanX = mean(:x), meanY = mean(:y))
x = @orderby(x, -:meanX)
x = @select(x, var = :b, :meanX, :meanY)

x1 = @linq transform(where(df, :a .> 2, :b .!= "c"), y = 10 * :x)
x1 = @linq by(x1, :b, meanX = mean(:x), meanY = mean(:y))
x1 = @linq select(orderby(x1, -:meanX), var = :b, :meanX, :meanY)

## chaining
xlinq = @linq df  |>
    where(:a .> 2, :b .!= "c")  |>
    transform(y = 10 * :x)  |>
    by(:b, meanX = mean(:x), meanY = mean(:y))  |>
    orderby(-:meanX)  |>
    select(var = :b, :meanX, :meanY)

@test x == x1
@test x == xlinq

xlinq2 = @linq df  |>
    where(:a .> 2, :b .!= "c")  |>
    transform(y = 10 * :x)  |>
    groupby(:b) |>
    orderby(-mean(:x))  |>
    based_on(meanX = mean(:x), meanY = mean(:y))

@test xlinq2[!, [:meanX, :meanY]] == xlinq[!, [:meanX, :meanY]]

xlinq3 = @linq df  |>
    where(:a .> 2, :b .!= "c")  |>
    transform(y = 10 * :x)  |>
    DataFrames.groupby(:b) |>
    orderby(-mean(:x))  |>
    based_on(meanX = mean(:x), meanY = mean(:y))

@test xlinq3[!, [:meanX, :meanY]] == xlinq[!, [:meanX, :meanY]]

@test (@linq df |> with(:a)) == df.a

end
