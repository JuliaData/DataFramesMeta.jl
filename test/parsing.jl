module TestParsing

using Test
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "Returning columnn identifiers" begin
    macro protect(x)
        esc(DataFramesMeta.get_column_expr(x))
    end

    v_sym = @protect $[:a, :b]
    @test v_sym == [:a, :b]

    v_str = @protect $["a", "b"]
    @test v_str == ["a", "b"]

    qn = @protect :x
    @test qn == :x

    qn_protect = @protect $:x
    @test qn_protect == :x

    str_protect = @protect $"x"
    @test qn_protect == "x"

    x = "a"
    sym_protect = @protect $x
    @test sym_protect === "a"

    v = ["a", "b"]
    v_protect = @protect $v
    @test v === v_protect

    b = @protect $(Between(:a, :b))
    @test b == Between(:a, :b)

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

end # module