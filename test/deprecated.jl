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
    @test @based_on(gd, n = first(Symbol.(:y, syms(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(:y, syms(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = :i).body == df.i
    @test @based_on(gd, transform = :i).transform == df.i
    @test @based_on(gd, (n1 = [first(:i)], n2 = [first(:y)])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yq), syms(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yq), syms(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(iq)).body == df.i
    @test @based_on(gd, transform = cols(iq)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(iq))], n2 = [first(cols(yq))])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(ir))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yr), syms(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yr), syms(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(ir)).body == df.i
    @test @based_on(gd, transform = cols(ir)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(ir))], n2 = [first(cols(yr))])).n1 == [1, 4]
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

# `y` needs to be in global scope here because the testset relies on `y`
y = 0
@testset "byrow" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    @test @byrow(df, if :A > :B; :A = 0 end) == DataFrame(A = [1, 0, 0], B = [2, 1, 2])

    @test  df == DataFrame(A = [1, 2, 3], B = [2, 1, 2])

    df = DataFrame(A = 1:3, B = [2, 1, 2])  # Restore df
    @byrow(df, if :A + :B == 3; global y += 1 end)
    @test  y == 2

    df = DataFrame(A = 1:3, B = [2, 1, 2])
    df2 = @byrow df begin
        @newcol colX::Array{Float64}
        @newcol colY::Array{Float64}
        :colX = :B == 2 ? pi * :A : :B
        if :A > 1
            :colY = :A * :B
        end
    end

    @test  df2.colX == [pi, 1.0, 3pi]
    @test  df2[2, :colY] == 2

    df = DataFrame(b = [5], a = [(n1 = 1, n2 = 2)])
    df2 = @byrow df begin
        :b = :a.n1
    end

    @test df2.b == [1]

    _DF = 5
    _N = 0
    @byrow df begin
        # nothing
    end
    @test _DF == 5
    @test _N == 0

    df3 = @byrow df begin
        :b = row
    end
    @test df3.b == [1]

    x = (a = 400, b = 600)
    df4 = @byrow df begin
        :b = x.a
    end
    @test df4.b == [400]
end

@testset "cols with @byrow" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])
    n = :A
    df2 = @byrow df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    n = "A"
    df2 = @byrow df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    df2 = @byrow df begin
        :A = cols(:A) + cols("B")
    end
    @test df2.A == df.A + df.B

    n = :A
    df2 = @byrow df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = "A"
    df2 = @byrow df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = :C
    df2 = @byrow df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])

    n = "C"
    df2 = @byrow df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])
end

df = DataFrame(A = 1:3, B = [2, 1, 2])

# `y` needs to be in global scope here because the testset relies on `y`
y = 0
@testset "byrow!" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    @test @byrow!(df, if :A > :B; :A = 0 end) == DataFrame(A = [1, 0, 0], B = [2, 1, 2])

    @test  df == DataFrame(A = [1, 2, 3], B = [2, 1, 2])

    df = DataFrame(A = 1:3, B = [2, 1, 2])  # Restore df
    @byrow!(df, if :A + :B == 3; global y += 1 end)
    @test  y == 2

    df = DataFrame(A = 1:3, B = [2, 1, 2])
    df2 = @byrow! df begin
        @newcol colX::Array{Float64}
        @newcol colY::Array{Float64}
        :colX = :B == 2 ? pi * :A : :B
        if :A > 1
            :colY = :A * :B
        end
    end

    @test  df2.colX == [pi, 1.0, 3pi]
    @test  df2[2, :colY] == 2

    df = DataFrame(b = [5], a = [(n1 = 1, n2 = 2)])
    df2 = @byrow! df begin
        :b = :a.n1
    end

    @test df2.b == [1]

    _DF = 5
    _N = 0
    @byrow! df begin
        # nothing
    end
    @test _DF == 5
    @test _N == 0

    df3 = @byrow! df begin
        :b = row
    end
    @test df3.b == [1]

    x = (a = 400, b = 600)
    df4 = @byrow! df begin
        :b = x.a
    end
    @test df4.b == [400]
end

@testset "cols with @byrow!" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])
    n = :A
    df2 = @byrow! df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    n = "A"
    df2 = @byrow! df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    df2 = @byrow! df begin
        :A = cols(:A) + cols("B")
    end
    @test df2.A == df.A + df.B

    n = :A
    df2 = @byrow! df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = "A"
    df2 = @byrow! df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = :C
    df2 = @byrow! df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])

    n = "C"
    df2 = @byrow! df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])
end

df = DataFrame(a = [:A, :B])
@testset "^ instead of syms" begin
    # To add when testset with syms is finalized.
end



end # module