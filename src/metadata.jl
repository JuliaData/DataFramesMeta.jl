function get_lhs_rhs(e)
    if !(e isa Expr)
        throw(ArgumentError("Malformed @label expression"))
    else
        lhs = let t = e.args[1]
            s = get_column_expr_rename(t)
            if s === nothing
                throw(ArgumentError("Invalid column identifier on LHS in @label macro"))
            end
            s
        end
        rhs = e.args[2]
        return lhs, rhs
    end
end

function addlabel_helper(df, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(df, args...; wrap_byrow = false)
    x_sym = gensym()
    t = map(exprs) do e
        lhs, rhs = get_lhs_rhs(e)
        :($label!($x_sym, $lhs, $rhs))
    end
    labblock = Expr(:block, t...)
    quote
        $x_sym = $x
        $labblock
        $x_sym
    end
end

"""
    label!(df, args...)

Assign labels to columns in a data frame using `:col = label` syntax.
Shorthand for `label!(df, ...)` from TablesMetaDataTools.jl.

```julia-repl
julia> df = DataFrame(wage = 12);

julia> @label! df :wage = "Wage per hour (USD)";

julia> printlabels(df)
┌────────┬─────────────────────┐
│ Column │               Label │
├────────┼─────────────────────┤
│   wage │ Wage per hour (USD) │
└────────┴─────────────────────┘
```

Use `@label!` for short descriptions, primarily for pretty printing.
Use `@note!` for longer explanations of columns.

Labels are "note"-style columnar metadata. Labels are preserved upon
renaming and transformations. `@label! :x = "Lab"` over-writes any
existing label for the column `:x`. To add information, see [`@note`](@ref).

`@label!` returns the input data frame for use with `@chain`.

Like other DataFramesMeta.jl macros, `@label!` can be used in "keyword"
format as well as block format.

```julia-repl
julia> df = DataFrame(wage = 12, tenure = 4);

julia> @label! df begin
           :wage = "Wage per hour (USD)"
           :tenure = "Tenure at job (months)"
       end;

julia> printlabels(df)
┌────────┬────────────────────────┐
│ Column │                  Label │
├────────┼────────────────────────┤
│   wage │    Wage per hour (USD) │
│ tenure │ Tenure at job (months) │
└────────┴────────────────────────┘
```
"""
macro label!(df, args...)
    esc(addlabel_helper(df, args...))
end

function addnote_helper(df, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(df, args...; wrap_byrow = false)
    x_sym = gensym()
    t = map(exprs) do e
        lhs, rhs = get_lhs_rhs(e)
        :($note!($x_sym, $lhs, string($rhs); append = true))
    end
    labblock = Expr(:block, t...)
    quote
        $x_sym = $x
        $labblock
        $x_sym
    end
end

"""
    note!(df, args...)

Assign notes to columns in a data frame using `:col = note` syntax.
Shorthand for `note!(df, col, note)` from TablesMetadataTools.jl.

Use `@note!` for longer explanations of columns.
Use `@label!` for short descriptions, primarily for pretty printing.

```julia-repl
julia> df = DataFrame(wage = 12);

julia> @note! df :wage = "
    Long discussion of variable construction.
     ";

julia> printnotes(df)
Column: wage
────────────
Long discussion of variable construction.
```

Unlike labels, notes are appended.

```julia-repl
julia> @note! df :wage = "Another comment on variable construction";

julia> printnotes(df)
Column: wage
────────────
Wage per hour in 2014 USD taken from ACS data provided by IPUMS.
Wage per hour is measured directly for hourly workers. For
salaried workers, equal to salary / hours worked.

Values capped at the 99th percentile
```
"""
macro note!(df, args...)
    esc(addnote_helper(df, args...))
end

function printlabels(df; all = true)
    d = colmetadata(df)
    t = DataFrame(Column = names(df))
    t.Label = labels(df)
    if all == false
        t = t[t.Label .!= t.Column, :]
    end
    pretty_table(t; show_subheader = false)
    return nothing
end

function printnotes(df)
    # "Column: " has 8 characters
    L = maximum(length.(names(df))) + 8
    for n in names(df)
        nt = note(df, n)
        if nt != ""
            println("Column: $n")
            println(repeat("─", L))
            println(nt)
        end
    end
end