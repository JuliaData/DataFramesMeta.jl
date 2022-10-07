module TestUnique

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "unique" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    @test @unique(df) ≅ unique(df)

    @test @unique(df, :A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @unique(df).A isa Vector{Union{Missing,Int}}

    @test @unique(df, $:A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @unique(df, $:A.+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @unique(df, :A.+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    @test @unique(df, $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @unique(subdf) ≅ DataFrame(A=[1, missing], B=[1, 1])


end

@testset "runique" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    @test @runique(df) ≅ unique(df)

    @test @runique(df, :A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @runique(df).A isa Vector{Union{Missing,Int}}

    @test @runique(df, $:A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @runique(df, $:A+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @runique(df, :A+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    @test @runique(df, $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @runique(subdf) ≅ DataFrame(A=[1, missing], B=[1, 1])


end


@testset "unique with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    d = @unique df begin
        :A .+ :B
    end

    @test d ≅ @unique df :A .+ :B

    d = @unique df begin
        :A
    end
    @test d ≅ @unique df :A

    subdf = @view df[df.B.==1, :]
    d = @unique subdf begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])
end


@testset "runique with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    d = @runique df begin
        :A + :B
    end

    @test d ≅ @runique df :A + :B

    d = @runique df begin
        :A
    end
    @test d ≅ @runique df :A

    subdf = @view df[df.B.==1, :]
    d = @runique subdf begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])
end


@testset "unique!" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = copy(df)

    @test @unique!(df2, :A) === df2

    @test @unique!(copy(df)) ≅ unique!(df)

    @test @unique!(copy(df), :A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @unique!(copy(df)).A isa Vector{Union{Missing,Int}}

    @test @unique!(copy(df), $:A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @unique!(copy(df), $:A.+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @unique!(copy(df), :A.+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    @test @unique!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @unique!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @unique(copy(subdf)) ≅ DataFrame(A=[1, missing], B=[1, 1])

end


@testset "runique!" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = copy(df)

    @test @runique!(df2, :A) === df2

    @test @runique!(copy(df)) ≅ unique!(df)

    @test @runique!(copy(df), :A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @runique!(copy(df)).A isa Vector{Union{Missing,Int}}

    @test @runique!(copy(df), $:A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @runique!(copy(df), $:A+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @runique!(copy(df), :A+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    @test @runique!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @runique!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @runique(copy(subdf)) ≅ DataFrame(A=[1, missing], B=[1, 1])

end


@testset "unique! with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    d = @unique! copy(df) begin
        :A .+ :B
    end
    @test d ≅ @unique!(df, :A .+ :B)

    d = @unique! copy(df) begin
        :A
    end
    @test d ≅ unique!(df, :A)

    subdf = @view df[df.B.==1, :]

    d = @unique! copy(subdf) begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])
end


@testset "runique! with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    d = @runique! copy(df) begin
        :A + :B
    end
    @test d ≅ @runique!(df, :A + :B)

    d = @runique! copy(df) begin
        :A
    end
    @test d ≅ @runique!(df, :A)

    subdf = @view df[df.B.==1, :]

    d = @runique! copy(subdf) begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])
end

end # module
