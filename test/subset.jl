module TestSubset

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "subset" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    x = [2, 1, 0, 0]

    @test @subset(df, :A .> 1) == df[(df.A .> 1) .=== true,:]
    @test @subset(df, :B .> 1) == df[df.B .> 1,:]
    @test @subset(df, :A .> x) == df[(df.A .> x) .=== true,:]
    @test @subset(df, :B .> x) ≅ df[df.B .> x,:]
    @test @subset(df, :A .> :B, :B .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @subset(df, :A .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @subset(df, :A .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @subset(df, :A .> 1).A isa Vector{Union{Missing, Int}}

    @test @subset(df, cols(:A) .> 1) == df[(df.A .> 1) .=== true,:]
    @test @subset(df, cols(:B) .> 1) == df[df.B .> 1,:]
    @test @subset(df, cols(:A) .> x) == df[(df.A .> x) .=== true,:]
    @test @subset(df, cols(:B) .> x) ≅ df[df.B .> x,:]
    @test @subset(df, cols(:A) .> :B, cols(:B) .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @subset(df, cols(:A) .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @subset(df, cols(:A) .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @subset(df, :A .> 1, :A .<= 2) == DataFrame(A = 2, B = 1)

    subdf = @view df[df.B .== 2, :]

    @test @subset(subdf, :A .== 3) == DataFrame(A = 3, B = 2)
end

@testset "subset with :block" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @subset df begin
        :A .> 1
        :B .> 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @subset df begin
        cols(:A) .> 1
        :B .> 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @subset df begin
        :A .> 1
        cols(:B) .> 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @subset df begin
        begin
            :A .> 1
        end
        :B .> 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @subset df begin
        :A .> 1
        @. :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)
end


@testset "subset!" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    x = [2, 1, 0, 0]

    @test @subset!(copy(df), :A .> 1) == df[(df.A .> 1) .=== true,:]
    @test @subset!(copy(df), :B .> 1) == df[df.B .> 1,:]
    @test @subset!(copy(df), :A .> x) == df[(df.A .> x) .=== true,:]
    @test @subset!(copy(df), :B .> x) ≅ df[df.B .> x,:]
    @test @subset!(copy(df), :A .> :B, :B .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @subset!(copy(df), :A .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @subset!(copy(df), :A .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @subset!(copy(df), :A .> 1).A isa Vector{Union{Missing, Int}}

    @test @subset!(copy(df), cols(:A) .> 1) == df[(df.A .> 1) .=== true,:]
    @test @subset!(copy(df), cols(:B) .> 1) == df[df.B .> 1,:]
    @test @subset!(copy(df), cols(:A) .> x) == df[(df.A .> x) .=== true,:]
    @test @subset!(copy(df), cols(:B) .> x) ≅ df[df.B .> x,:]
    @test @subset!(copy(df), cols(:A) .> :B, cols(:B) .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @subset!(copy(df), cols(:A) .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @subset!(copy(df), cols(:A) .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @subset!(copy(df), :A .> 1, :A .<= 2) == DataFrame(A = 2, B = 1)

    subdf = @view df[df.B .== 2, :]

    @test @subset!(copy(subdf), :A .== 3) == DataFrame(A = 3, B = 2)
end

@testset "subset! with :block" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @subset! copy(df) begin
        :A .> 1
        :B .> 1
    end
    @test d ≅ @subset!(copy(df), :A .> 1, :B .> 1)

    d = @subset! copy(df) begin
        cols(:A) .> 1
        :B .> 1
    end
    @test d ≅ @subset!(copy(df), :A .> 1, :B .> 1)

    d = @subset! copy(df) begin
        :A .> 1
        cols(:B) .> 1
    end
    @test d ≅ @subset!(copy(df), :A .> 1, :B .> 1)

    d = @subset! copy(df) begin
        begin
            :A .> 1
        end
        :B .> 1
    end
    @test d ≅ @subset!(copy(df), :A .> 1, :B .> 1)

    d = @subset! copy(df) begin
        :A .> 1
        @. :B > 1
    end
    @test d ≅ @subset!(copy(df), :A .> 1, :B .> 1)
end

end # module