module Testdistinct

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "distinct" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))

    @test @distinct(df) ≅ unique(df)

    @test @distinct(df, $"A" .+ $"B") ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct(df, :A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct(df, :A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct(df, $[:A,:B]) ≅ unique(df, [:A, :B])
    @test @distinct(df).A isa Vector{Union{Missing,Int}}

    @test @distinct(df, $:A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct(df, $:A.+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct(df, :A.+$:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    @test @distinct(df, $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @distinct(subdf) ≅ DataFrame(A=[1, missing], B=[1, 1])

    tmp = select(df2, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df2[rowidxs, :]

    @test @distinct(df2, :x .+ :y, :z .+ 1) ≅ res

end

@testset "rdistinct" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))

    @test @rdistinct(df, $"A" + $"B") ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct(df) ≅ unique(df)

    @test @rdistinct(df, :A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct(df).A isa Vector{Union{Missing,Int}}

    @test @rdistinct(df, $:A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct(df, $:A + $:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct(df, :A + $:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    @test @rdistinct(df, $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @rdistinct(subdf) ≅ DataFrame(A=[1, missing], B=[1, 1])

    tmp = select(df2, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df2[rowidxs, :]

    @test @rdistinct(df2, :x + :y, :z + 1) ≅ res



end


@testset "distinct with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))

    d = @distinct df begin
        $"A" .+ $"B"
    end

    @test d ≅ @distinct df :A .+ :B

    d = @distinct df begin
        :A .+ :B
    end

    @test d ≅ @distinct df :A .+ :B

    d = @distinct df begin
        :A
    end
    @test d ≅ @distinct df :A

    subdf = @view df[df.B.==1, :]
    d = @distinct subdf begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])

    tmp = select(df2, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df2[rowidxs, :]

    d = @distinct df2 begin 
        :x .+ :y 
        :z .+ 1
    end
    @test d ≅ res

end


@testset "rdistinct with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))

    d = @rdistinct df begin
        :A + :B
    end

    @test d ≅ @rdistinct df :A + :B

    d = @rdistinct df begin
        $"A" + $"B"
    end

    @test d ≅ @rdistinct df :A + :B

    d = @rdistinct df begin
        :A
    end
    @test d ≅ @rdistinct df :A

    subdf = @view df[df.B.==1, :]
    d = @rdistinct subdf begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])

    tmp = select(df2, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df2[rowidxs, :]
    
    d = @rdistinct df2 begin
        :x + :y
        :z + 1 
    end
    @test d ≅ res

end


@testset "distinct!" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = copy(df)

    @test @distinct!(df2, :A) === df2

    @test @distinct!(copy(df), $"A" .+ $"B") ≅ unique!(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct!(copy(df)) ≅ unique!(df)

    @test @distinct!(copy(df), :A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct!(copy(df)).A isa Vector{Union{Missing,Int}}

    @test @distinct!(copy(df), $:A .+ :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct!(copy(df), $:A .+ $:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct!(copy(df), :A .+ $:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct!(df, $[:A,:B]) ≅ unique!(df, [:A, :B])

    @test @distinct!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @distinct!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @distinct(copy(subdf)) ≅ DataFrame(A=[1, missing], B=[1, 1])

    df3 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))
    tmp = select(df3, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df3[rowidxs, :]
    
    @test @distinct!(copy(df3), :x .+ :y, :z .+ 1) ≅ res
    
end


@testset "rdistinct!" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])
    df2 = copy(df)

    @test @rdistinct!(df2, :A) === df2
    @test @rdistinct!(df, $"A" + $"B") ≅ unique!(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct!(copy(df)) ≅ unique!(df)

    @test @rdistinct!(copy(df), :A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct!(copy(df)).A isa Vector{Union{Missing,Int}}

    @test @rdistinct!(copy(df), $:A + :B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct!(copy(df), $:A + $:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct!(copy(df), :A + $:B) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    @test @rdistinct!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)
    @test @rdistinct!(copy(df), $([:A, :B] => (x, y) -> x .+ y)) ≅ unique(df, [:A, :B] => (x, y) -> x .+ y)

    subdf = @view df[df.B.==1, :]
    @test @rdistinct(copy(subdf)) ≅ DataFrame(A=[1, missing], B=[1, 1])

    df3 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))
    tmp = select(df3, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df3[rowidxs, :]
    
    @test @rdistinct!(copy(df3), :x + :y, :z + 1) ≅ res

end


@testset "distinct! with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    d = @distinct! copy(df) begin
        :A .+ :B
    end
    @test d ≅ @distinct!(df, :A .+ :B)

    d = @distinct! copy(df) begin
        $"A" .+ $"B"
    end
    @test d ≅ @distinct!(df, :A .+ :B)

    d = @distinct! copy(df) begin
        :A
    end
    @test d ≅ @distinct!(df, :A)

    subdf = @view df[df.B.==1, :]

    d = @distinct! copy(subdf) begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])

    df2 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))
    tmp = select(df2, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df2[rowidxs, :]
    
    
    d = @distinct! df2 begin
        :x .+ :y
        :z .+  1 
    end
        
    d ≅ res
end


@testset "rdistinct! with :block" begin
    df = DataFrame(A=[1, 1, 3, missing], B=[1, 1, 2, 1])

    d = @rdistinct! copy(df) begin
        :A + :B
    end
    @test d ≅ @rdistinct!(df, :A + :B)

    d = @rdistinct! copy(df) begin
        $"A" + $"B"
    end
    @test d ≅ @rdistinct!(df, :A + :B)

    d = @rdistinct! copy(df) begin
        :A
    end
    @test d ≅ @rdistinct!(df, :A)

    subdf = @view df[df.B.==1, :]

    d = @rdistinct! copy(subdf) begin
        :A
    end

    @test d ≅ DataFrame(A=[1, missing], B=[1, 1])

    df2 = DataFrames.DataFrame(x = 1:10,y = 10:-1:1, z = rand(0:1,10))
    tmp = select(df2, [:x,:y] => (x,y) -> (x .+ y), :z => (z)-> z .+1; copycols = false)
    rowidxs = (!).(nonunique(tmp))
    res = df2[rowidxs, :]
    
    
    d = @rdistinct! df2 begin
        :x + :y
        :z +  1 
    end
        
    d ≅ res
end

end # module
