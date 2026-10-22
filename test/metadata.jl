module TestMetaData

using Test, DataFramesMeta

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

function captured_stdout(f)
    mktemp() do _, io
        result = redirect_stdout(f, io)
        flush(io)
        seekstart(io)
        return result, read(io, String)
    end
end
@testset "metadata output streams" begin
    df = DataFrame(wage = [12], age = [23], extra = [1])
    @label! df :wage = "Wage <USD>"
    @label! df :age = "Age"
    @note! df :wage = "First line\nSecond line"
    for (printer, options) in ((printlabels, (;)),
                               (printlabels, (; unlabelled = false)),
                               (printnotes, (;)),
                               (printnotes, (; unnoted = true)))
        @testset "$(printer), $(options)" begin
            old_result, expected = captured_stdout(() -> printer(df; options...))
            buffer = IOBuffer()
            result, leaked = captured_stdout(() -> printer(buffer, df; options...))
            @test result === old_result === nothing
            @test isempty(leaked)
            @test String(take!(buffer)) == expected
        end
    end
    @testset "selected labels" begin
        text = sprint(io -> printlabels(io, df, :wage))
        @test occursin("Wage <USD>", text)
        @test !occursin("Age", text)
    end
    @testset "selected notes" begin
        text = sprint(io -> printnotes(io, df, :wage))
        @test text == "Column: wage\n────────────\nLabel: Wage <USD>\nFirst line\nSecond line\n\n"
    end
    @testset "HTML formatter" begin
        backend = pkgversion(DataFramesMeta.PrettyTables).major == 2 ? Val(:html) : :html
        html = sprint(io -> printlabels(io, df; backend))
        @test occursin("<table", html)
        @test occursin("Wage &lt;USD&gt;", html)
    end
    @testset "empty notes and empty frames" begin
        @test isempty(sprint(io -> printnotes(io, DataFrame(a = [1]))))
        @test isempty(sprint(io -> printnotes(io, DataFrame())))
        @test printlabels(IOBuffer(), DataFrame()) === nothing
    end
end

end # module
