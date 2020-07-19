

module TestGrouping

using Test
using DataFrames
using DataFramesMeta
using Statistics
using Tables

const ≅ = isequal

d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x, sort=true)

@test  @where(d, :x .== 3) == DataFramesMeta.where(d, x -> x.x .== 3)
@test  DataFrame(@where(g, length(:x) > 5)) == DataFrame(DataFramesMeta.where(g, x -> length(x.x) > 5))
@test  DataFrame(@where(g, length(:x) > 5))[!, :n][1:3] == [5, 6, 7]

@test  DataFrame(DataFramesMeta.orderby(g, x -> mean(x.n))) == DataFrame(@orderby(g, mean(:n)))

@test @based_on(g, nsum = sum(:n)).nsum == [99, 84, 27]

df = DataFrame(
    g = [1, 1, 1, 2, 2],
    i = 1:5, 
    t = ["a", "b", "c", "c", "e"],
    y = [:v, :w, :x, :y, :z],
    c = [:g, :quote, :body, :transform, missing]
    )

m = [100, 200, 300, 400, 500]

gq = :g
iq = :i
tq = :t
yq = :y
cq = :c

gr = "g"
ir = "i"
tr = "t"
yr = "y"
cr = "c"

gd = groupby(df, :g)

@testset "@based_on" begin
    @test @based_on(gd, n = mean(:i)).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(:i) + mean(:g)).n == [3.0, 6.5]
    @test @based_on(gd, n = first(:t .* string.(:y))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = :i).body == df.i 
    @test @based_on(gd, transform = :i).transform == df.i
    @test @based_on(gd, (n1 = [first(:i)], n2 = [first(:y)])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(iq)).body == df.i 
    @test @based_on(gd, transform = cols(iq)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(iq))], n2 = [first(cols(yq))])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(ir))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(ir)).body == df.i 
    @test @based_on(gd, transform = cols(ir)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(ir))], n2 = [first(cols(yr))])).n1 == [1, 4]
end

@testset "Limits of @based_on" begin
    @test_throws LoadError @eval @based_on(gd, :i)
    @test @based_on(gd, [:i, :g]) ≅ DataFrame(g = df.g, x1 = df.i, x2 = df.g)
    @test_throws ArgumentError @eval @based_on(gd, All())
    @test_throws MethodError @eval @based_on(gd, Between(:i, :t)).Between == df.i
    @test_throws ArgumentError @eval @based_on(gd, Not(:i)).Not == df.i
    @test_throws ArgumentError @eval @based_on(gd, Not([:i, :g]))
    newvar = :n
    @test_throws ArgumentError @eval @based_on(gd, cols(newvar) = mean(:i))
    @test_throws MethodError @eval @based_on(gd, n = sum(Between(:i, :t)))
    @test_throws LoadError @eval @based_on(gd; n = mean(:i))
end



@testset "@transform with grouped data frame" begin
	d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
	g = groupby(d, :x, sort=true)
	
	@test  (@transform(g, y = :n .- median(:n)))[1,:y] == -5.0

	d = DataFrame(a = [1,1,1,2,2,3,3,1],
	              b = Any[1,2,3,missing,missing,6.0,5.0,4],
	              c = CategoricalArray([1,2,3,1,2,3,1,2]))
	g = groupby(d, :a)
	## Scalar output
	# Type promotion Int -> Float
	t = @transform(g, t = :b[1]).t
	@test isequal(t, [1.0, 1.0, 1.0, 1.0, missing, missing, 6.0, 6.0]) &&
	      t isa Vector{Union{Float64, Missing}}
	# Type promotion Number -> Any
	t = @transform(g, t = isequal(:b[1], 1) ? :b[1] : "a").t
	@test isequal(t, [1, 1, 1, 1, "a", "a", "a", "a"]) &&
	      t isa Vector{Any}
	## Vector output
	# Normal use
	t = @transform(g, t = :b .- mean(:b)).t
	@test isequal(t, [-1.5, -0.5, 0.5, 1.5, missing, missing, 0.5, -0.5]) &&
	      t isa Vector{Union{Float64, Missing}}
	# Type promotion
	t = @transform(g, t = isequal(:b[1], 1) ? fill(1, length(:b)) : fill(2.0, length(:b))).t
	@test isequal(t, [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0]) &&
	      t isa Vector{Float64}
	# Vectors whose eltypes promote to any
	t = @transform(g, t = isequal(:b[1], 1) ? :b : fill("a", length(:b))).t
	@test isequal(t, [1, 2, 3, 4, "a", "a", "a", "a"]) &&
	      t isa Vector{Any}
	# Categorical Array
	# Scalar
	t = @transform(g, t = :c[1]).t
	@test isequal(t, [1, 1, 1, 1, 1, 1, 3, 3]) &&
	      t isa CategoricalVector{Int}
	# Vector
	t = @transform(g, t = :c).t
	@test isequal(t, [1, 2, 3, 2, 1, 2, 3, 1]) &&
	      t isa CategoricalVector{Int}
end

end # module
