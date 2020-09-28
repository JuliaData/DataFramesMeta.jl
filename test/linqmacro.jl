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

@testset "@linq with `cols`" begin
    df = DataFrame(
            a = [1, 2, 3, 4],
            b = ["a", "b", "c", "d"],
            x = [10, 20, 30, 40],
            y = [40, 50, 60, 70]
        )

    a_sym = :a
    b_str = "b"
    x_sym = :x
    y_str = "y"
    xlinq3 = @linq df  |>
        where(cols(a_sym) .> 2, :b .!= "c")  |>
        transform(cols(y_str) = 10 * cols(x_sym))  |>
        DataFrames.groupby(b_str) |>
        orderby(-mean(cols(x_sym)))  |>
        based_on(cols("meanX") = mean(:x), meanY = mean(:y))

    @test isequal(xlinq3, DataFrame(b = "d", meanX = 40.0, meanY = 400.0))
end

end # module
