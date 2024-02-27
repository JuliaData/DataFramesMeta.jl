module TestMetaData

using Test

@testset "labels" begin
    df = DataFrame(a = 1, b = 2)
    @label! df :a = "alab"
    @test labels(df) == ["alab", "b"]

    df = DataFrame(a = 1, b = 2)
    @label! df begin
        :a = "alab"
        :b = "blab"
    end
    @test labels(df) == ["alab", "blab"]

    df_new = leftjoin(DataFrame(a = 1, c = 2), df, on = :a)
    @test labels(df_new) == ["a", "c", "blab"]

    df_new = @rename df :a2 = :a
    @test labels(df_new) == ["alab", "blab"]

    df_new = @rtransform df :a = :a + 1
    @test labels(df_new) == ["alab", "blab"]
end

@testset "notes" begin
    df = DataFrame(a = 1, b = 2)
    @note! df :a = "anote"
    @test note(df, :a) == "anote"

    @note! df :a = "anote2"
    @test note(df, :a) == "anote\nanote2"

    df = DataFrame(a = 1, b = 2)
    @note! df begin
        :a = "anote"
        :b = "bnote"
    end
    @test note(df, :a) == "anote"
    @test note(df, :b) == "bnote"

    df_new = leftjoin(DataFrame(a = 1, c = 2), df, on = :a)
    @test note(df_new, :a) == ""
    @test note(df_new, :b) == "bnote"

    df_new = @rename df :a2 = :a
    @test note(df_new, :a2) == "anote"
    @test note(df_new, :b) == "bnote"

    df_new = @rtransform df :a = :a + 1
    @test note(df_new, :a) == "anote"
    @test note(df_new, :b) == "bnote"
end

@testset "Metadata printing" begin
    df = DataFrame(a = [1], b = [2])
    @label! df :a = "A label"
    @note! df :a = "A note"

    # Just confirm the printing doesn't error
    printlabels(df)
    printlabels(df, :a)
    printlabels(df, [:a, :b])
    printlabels(df; unlabelled = true)
    printlabels(df; unlabelled = false)
    printlabels(df, [:a, :b], unlabelled = false)
    printlabels(df, [:a, :b], unlabelled = true)

    printnotes(df)
    printnotes(df, :a)
    printnotes(df, [:a, :b])
    printnotes(df; unnoted = true)
    printnotes(df; unnoted = false)
    printnotes(df, [:a, :b], unnoted = false)
    printnotes(df, [:a, :b], unnoted = true)
end

end # module