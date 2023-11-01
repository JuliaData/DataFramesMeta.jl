module Deprecated

using Test
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@based_on" begin
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

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @based_on(gd, n = mean(:i)).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(:i) + mean(:g)).n == [3.0, 6.5]
    @test @based_on(gd, n = first(:t .* string.(:y))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = :i).body == df.i
    @test @based_on(gd, transform = :i).transform == df.i

    @test @based_on(gd, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(iq)).body == df.i
    @test @based_on(gd, transform = cols(iq)).transform == df.i

    @test @based_on(gd, n = mean(cols(ir))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(ir)).body == df.i
    @test @based_on(gd, transform = cols(ir)).transform == df.i
    @test @based_on(gd, n = mean(cols("i")) + 0 * first(cols(:g))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(2)) + first(cols(1))).n == [3.0, 6.5]


    @test @based_on(gd, :i) == select(df, :g, :i)
    @test @based_on(gd, :i, :g) ≅ select(df, :g, :i)

    @test @based_on(gd, :i, n = 1).n == fill(1, nrow(df))

    @test @based_on(gd, cols("new_column") = 2).new_column == [2, 2]
    @test @based_on(gd, cols(n_str) = 2).new_column == [2, 2]
    @test @based_on(gd, cols(n_sym) = 2).new_column == [2, 2]
    @test @based_on(gd, cols(n_space) = 2)."new column" == [2, 2]
    @test @based_on(gd, cols("new" * "_" * "column") = 2)."new_column" == [2, 2]
end

@testset "where" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    x = [2, 1, 0, 0]

    @test @where(df, :A .> 1) == df[(df.A .> 1) .=== true,:]
    @test @where(df, :B .> 1) == df[df.B .> 1,:]
    @test @where(df, :A .> x) == df[(df.A .> x) .=== true,:]
    @test @where(df, :B .> x) ≅ df[df.B .> x,:]
    @test @where(df, :A .> :B, :B .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @where(df, :A .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @where(df, :A .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @where(df, :A .> 1).A isa Vector{Union{Missing, Int}}

    @test @where(df, cols(:A) .> 1) == df[(df.A .> 1) .=== true,:]
    @test @where(df, cols(:B) .> 1) == df[df.B .> 1,:]
    @test @where(df, cols(:A) .> x) == df[(df.A .> x) .=== true,:]
    @test @where(df, cols(:B) .> x) ≅ df[df.B .> x,:]
    @test @where(df, cols(:A) .> :B, cols(:B) .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @where(df, cols(:A) .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @where(df, cols(:A) .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @where(df, :A .> 1, :A .<= 2) == DataFrame(A = 2, B = 1)

    subdf = @view df[df.B .== 2, :]

    @test @where(subdf, :A .== 3) == DataFrame(A = 3, B = 2)
end

@testset "where with :block" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @where df begin
        :A .> 1
        :B .> 1
    end
    @test d ≅ @where(df, :A .> 1, :B .> 1)

    d = @where df begin
        cols(:A) .> 1
        :B .> 1
    end
    @test d ≅ @where(df, :A .> 1, :B .> 1)

    d = @where df begin
        :A .> 1
        cols(:B) .> 1
    end
    @test d ≅ @where(df, :A .> 1, :B .> 1)

    d = @where df begin
        begin
            :A .> 1
        end
        :B .> 1
    end
    @test d ≅ @where(df, :A .> 1, :B .> 1)

    d = @where df begin
        :A .> 1
        @. :B > 1
    end
    @test d ≅ @where(df, :A .> 1, :B .> 1)
end

@testset "@where with a grouped data frame" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
    )

    gd = groupby(df, :g)

    @test @where(gd, :i .== first(:i)) ≅ df[[1, 4], :]
    @test @where(gd, cols(:i) .> mean(cols(:i)), :t .== "c") ≅ df[[3], :]
    @test @where(gd, :c .== :g) ≅ df[[], :]
end

@testset "Unquoted symbols on LHS" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
    )

    gd = groupby(df, :g)

    newdf = @transform df :n = :i

    @test (@transform df n = :i) ≅ newdf
    @test (@transform(df, n = identity(:i))) ≅ newdf
    @test (@transform df @byrow n = :i) ≅ newdf
    d = @transform df begin
        n = identity(:i)
    end
    @test d ≅ newdf

    d = @eachrow df begin
        @newcol n::Vector{Int}
        :n = :i
    end
    @test d ≅ newdf

    newdf = @select df :n = :i

    @test (@select df n = :i) ≅ newdf
    @test (@select(df, n = identity(:i))) ≅ newdf
    d = @select df begin
        n = identity(:i)
    end
    @test (@select df @byrow n = :i) ≅ newdf
    @test d ≅ newdf

    newdf = @combine gd :n = first(:i)
    @test (@combine gd n = first(:i)) ≅ newdf
    @test (@combine(gd, n = first(:i))) ≅ newdf
end

@testset "with" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    x = [2, 1, 0]

    @test  @with(df, :A .+ 1)   ==  df.A .+ 1
    @test  @with(df, :A .+ :B)  ==  df.A .+ df.B
    @test  @with(df, :A .+ x)   ==  df.A .+ x

    x = @with df begin
        res = 0.0
        for i in 1:length(:A)
            res += :A[i] * :B[i]
        end
        res
    end
    idx = :A
    @test  @with(df, $idx .+ :B)  ==  df.A .+ df.B
    idx2 = :B
    @test  @with(df, $idx .+ $idx2)  ==  df.A .+ df.B
    @test  @with(df, $:A .+ $"B")  ==  df.A .+ df.B

    @test_throws ArgumentError @with(df, :A + $2)

    @test  x == sum(df.A .* df.B)
    @test  @with(df, df[:A .> 1, ^([:B, :A])]) == df[df.A .> 1, [:B, :A]]
    @test  @with(df, DataFrame(a = :A * 2, b = :A .+ :B)) == DataFrame(a = df.A * 2, b = df.A .+ df.B)

    @test @with(df, :A) === df.A
    @test @with(df, $:A) === df.A
    @test @with(df, $"A") === df.A
end


end # module