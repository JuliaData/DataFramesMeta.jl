module TestWhen

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@transform when" begin
    df = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(a = [1, 2], z = [60, 500], c = [missing, 5])
    df2 = @transform df begin
        @when :a .> 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @transform(df, @when(:a .> 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df2 = @transform df @byrow begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @transform df @byrow @passmissing begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @transform df begin
        @byrow @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @transform df begin
        @when @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res


    df2 = @transform df begin
        @when @byrow :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    dfa = copy(df)
    dfa.a = [missing, 2]
    df2 = @transform dfa begin
        @when @passmissing @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ DataFrame(a = [missing, 2], z = [60, 500], c = [missing, 5])
end

@testset "@rtransform when" begin
    df = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(a = [1, 2], z = [60, 500], c = [missing, 5])
    df2 = @rtransform df begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @rtransform(df, @when(:a > 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df2 = @rtransform df @passmissing begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @rtransform df begin
        @when :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    dfa = copy(df)
    dfa.a = [missing, 2]
    df2 = @transform dfa begin
        @when @passmissing @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ DataFrame(a = [missing, 2], z = [60, 500], c = [missing, 5])
end

@testset "@transform! when" begin
    df_orig = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(a = [1, 2], z = [60, 500], c = [missing, 5])
    df = copy(df_orig)
    df2 = @transform! df begin
        @when :a .> 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @transform!(df, @when(:a .> 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df = copy(df_orig)
    df2 = @transform! df @byrow begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @transform! df @byrow @passmissing begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @transform! df begin
        @byrow @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @transform! df begin
        @when @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @transform! df begin
        @when @byrow :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    dfa = copy(df_orig)
    dfa.a = [missing, 2]
    df = copy(dfa)
    df2 = @transform! df begin
        @when @passmissing @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ DataFrame(a = [missing, 2], z = [60, 500], c = [missing, 5])
    @test df2 === df
end


@testset "@rtransform! when" begin
    df_orig = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(a = [1, 2], z = [60, 500], c = [missing, 5])
    df = copy(df_orig)
    df2 = @rtransform! df begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @rtransform!(df, @when(:a > 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df = copy(df_orig)
    df2 = @rtransform! df begin
        @when :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    dfa = copy(df_orig)
    dfa.a = [missing, 2]
    df = copy(dfa)
    df2 = @rtransform! df begin
        @when @passmissing :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ DataFrame(a = [missing, 2], z = [60, 500], c = [missing, 5])
    @test df2 === df
end

@testset "@select when" begin
    df = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(c = [missing, 5], z = [60, 500])
    df2 = @select df begin
        @when :a .> 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @select(df, @when(:a .> 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df2 = @select df @byrow begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @select df @byrow @passmissing begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @select df begin
        @byrow @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @select df begin
        @when @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res


    df2 = @select df begin
        @when @byrow :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    dfa = copy(df)
    dfa.a = [missing, 2]
    df2 = @select dfa begin
        @when @passmissing @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
end

@testset "@rselect when" begin
    df = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(c = [missing, 5], z = [60, 500])
    df2 = @rselect df begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @rselect(df, @when(:a > 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df2 = @rselect df @passmissing begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    df2 = @rselect df begin
        @when :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res

    dfa = copy(df)
    dfa.a = [missing, 2]
    df2 = @select dfa begin
        @when @passmissing @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
end

@testset "@select! when" begin
    df_orig = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(c = [missing, 5], z = [60, 500])
    df = copy(df_orig)
    df2 = @select! df begin
        @when :a .> 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @select!(df, @when(:a .> 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df = copy(df_orig)
    df2 = @select! df @byrow begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @select! df @byrow @passmissing begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @select! df begin
        @byrow @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @select! df begin
        @when @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @select! df begin
        @when @byrow :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    dfa = copy(df_orig)
    dfa.a = [missing, 2]
    df = copy(dfa)
    df2 = @select! df begin
        @when @passmissing @byrow :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df
end


@testset "@rselect! when" begin
    df_orig = DataFrame(a = [1, 2], z = [60, 70])
    res = DataFrame(c = [missing, 5], z = [60, 500])
    df = copy(df_orig)
    df2 = @rselect! df begin
        @when :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    df = copy(df_orig)
    df2 = @rselect!(df, @when(:a > 1), :c = 5, :z = 500)
    @test df2 ≅ res

    df = copy(df_orig)
    df2 = @rselect! df begin
        @when :a > 1 ? true : missing
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df

    dfa = copy(df_orig)
    dfa.a = [missing, 2]
    df = copy(dfa)
    df2 = @rselect! df begin
        @when @passmissing :a > 1
        :c = 5
        :z = 500
    end
    @test df2 ≅ res
    @test df2 === df
end

@testset "@when many conditions" begin
    df = DataFrame(a = [1, missing, 3, 4], z = [50, 60, 70, 80])
    @transform df begin
        @when :a .> 1
        @when :a .> 2
        :c = 5
    end

end


@testset "@with when" begin
    df = DataFrame(a = [1, 2], z = [60, 70])

    t = @with df begin
        @when :a .> 1
        :z
    end
    @test t === view(df.z, 2:2)
end

end # module