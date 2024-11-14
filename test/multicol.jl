module TestMultiCol

using Test
using DataFrames
using DataFramesMeta

df = DataFrame(A = 1, AA = 2, B = 3)

@testset "select_multi" begin
    df = DataFrame(A = 1, AA = 2, B = 3)

    t = @select df Not(:A)
    @test t == DataFrame(AA = 2, B = 3)

    t = @select df All()
    @test t == DataFrame(A = 1, AA = 2, B = 3)

    t = @select df Cols(r"A")
    @test t == DataFrame(A = 1, AA = 2)

    t = @select df Between(:AA, :B)
    @test t == DataFrame(AA = 2, B = 3)
end

@testset "rselect_multi" begin
    df = DataFrame(A = 1, AA = 2, B = 3)

    t = @rselect df Not(:A)
    @test t == DataFrame(AA = 2, B = 3)

    t = @rselect df All()
    @test t == DataFrame(A = 1, AA = 2, B = 3)

    t = @rselect df Cols(r"A")
    @test t == DataFrame(A = 1, AA = 2)

    t = @rselect df Between(:AA, :B)
    @test t == DataFrame(AA = 2, B = 3)
end

@testset "select!_multi" begin
    df = DataFrame(A = 1, AA = 2, B = 3)

    @select! df Not(:A)
    @test df == DataFrame(AA = 2, B = 3)

    df = DataFrame(A = 1, AA = 2, B = 3)

    @select! df All()
    @test df == DataFrame(A = 1, AA = 2, B = 3)

    df = DataFrame(A = 1, AA = 2, B = 3)

    @select! df Cols(r"A")
    @test df == DataFrame(A = 1, AA = 2)

    df = DataFrame(A = 1, AA = 2, B = 3)

    @select! df Between(:AA, :B)
    @test df == DataFrame(AA = 2, B = 3)
end

@testset "rselect!_multi" begin
    df = DataFrame(A = 1, AA = 2, B = 3)

    @rselect! df Not(:A)
    @test df == DataFrame(AA = 2, B = 3)

    df = DataFrame(A = 1, AA = 2, B = 3)

    @rselect! df All()
    @test df == DataFrame(A = 1, AA = 2, B = 3)

    df = DataFrame(A = 1, AA = 2, B = 3)

    @rselect! df Cols(r"A")
    @test df == DataFrame(A = 1, AA = 2)

    df = DataFrame(A = 1, AA = 2, B = 3)

    @rselect! df Between(:AA, :B)
    @test df == DataFrame(AA = 2, B = 3)
end


@testset "othermacros_multi" begin
    df = DataFrame(A = 1, AA = 2, B = 3)

    @test_throws LoadError @eval  @with df Not(:A)

    @test_throws LoadError @eval  @with df All()

    @test_throws LoadError @eval  @with df Cols(r"A")

    @test_throws LoadError @eval  @with df Between(:AA, :B)

    @test_throws LoadError @eval  @with(df, begin
        1
        Not(:A)
    end)

    @test_throws LoadError @eval  @with df begin
        1
        All()
    end

    @test_throws LoadError @eval  @with df begin
        1
        Cols(r"A")
    end

    @test_throws LoadError @eval  @with df begin
        1
        Between(:AA, :B)
    end
end

@testset "othermacros_multi" begin
    df = DataFrame(A = 1, AA = 2, B = 3)

    @test_throws LoadError @eval  @select df :y = Not(:A)

    @test_throws LoadError @eval  @select df :y = All()

    @test_throws LoadError @eval  @select df :y = Cols(r"A")

    @test_throws LoadError @eval  @select df :y = Between(:AA, :B)

    @test_throws LoadError @eval  @select(df, :y = begin
        1
        Not(:A)
    end)

    @test_throws LoadError @eval  @select df :y = begin
        1
        All()
    end

    @test_throws LoadError @eval  @select df :y = begin
        1
        Cols(r"A")
    end

    @test_throws LoadError @eval  @select df :y = begin
        1
        Between(:AA, :B)
    end
end


end # module