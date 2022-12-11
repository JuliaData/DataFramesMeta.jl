module TestParsing

using Test
using DataFramesMeta
using Statistics

const ≅ = isequal

macro protect(x)
    esc(DataFramesMeta.get_column_expr(x))
end

@testset "Returning columnn identifiers" begin

    v_sym = @protect $[:a, :b]
    @test v_sym == [:a, :b]

    v_str = @protect $["a", "b"]
    @test v_str == ["a", "b"]

    qn = @protect :x
    @test qn == :x

    qn_protect = @protect $:x
    @test qn_protect == :x

    str_protect = @protect $"x"
    @test str_protect == "x"

    x = "a"
    sym_protect = @protect $x
    @test sym_protect === "a"

    v = ["a", "b"]
    v_protect = @protect $v
    @test v === v_protect

    b = @protect $(Between(:a, :b))
    @test b == Between(:a, :b)

    b = @protect $(begin 1 end)
    @test b == 1

    c = @protect cols(:a)
    @test c == :a

    i = @protect $1
    @test i == 1

    n = @protect "hello"
    @test n === nothing

    n = @protect 1
    @test n === nothing

    n = @protect begin x end
    @test n === nothing

    n = @protect x
    @test n === nothing
end

@testset "broadcasted binary operators" begin
    df = DataFrame(x = [1, 2], y = [3, 4])

    df2 = @select df :z = first(:x .+ :y)
    @test df2 == DataFrame(z = [4, 4])

    df2 = @by df :x :y = first(:y .* :y)

    @test df2 == DataFrame(x = [1, 2], y = [9, 16])

    df2 = @select df :y = first(last(:y))

    @test df2 == DataFrame(y = [4, 4])

    df2 = @select df :z = .+(:x)

    @test df2 == DataFrame(z = [1, 2])

    df2 = @select df :z = .+(first(:x))

    @test df2 == DataFrame(z = [1, 1])

    df2 = @select df :z = first(.*(:x))

    @test df2 == DataFrame(z = 1)

    df2 = @select df :z = .+(.*(:x))

    @test df2 == DataFrame(z = 1)

    df2 = @select df :z = .+(.*(:x, :y))

    @test df2 == DataFrame(z = 2)

    @test df2 == DataFrame(y = 2)

    df = DataFrame(
        x = [1, 1, 2, 2, 3, 3],
        y = [true, false, true, false, true, false],
        z = [true, true, true, false, false, false])

    df2 = @by(df,
    :x,
    :a = maximum(:y .* :z))

    @test df2 == DataFrame(x = [1, 2, 3], a = [true, true, false])

end

end # module