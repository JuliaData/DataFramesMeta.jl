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
    @label!(df, args...)

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
existing label for the column `:x`. To add information without overwriting,
use [`@note!`](@ref).

Returns `df`, with the labels of `df` modified.

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
    @note!(df, args...)

Assign notes to columns in a data frame using `:col = note` syntax.
Shorthand for `note!(df, col, note)` from TablesMetadataTools.jl.

Use `@note!` for longer explanations of columns.
Use `@label!` for short descriptions, primarily for pretty printing.

Returns `df`, with the notes of `df` modified.

```julia-repl

julia> df = DataFrame(wage = 12);

julia> @note! df :wage = "
       Wage per hour in 2014 USD taken from ACS data provided by IPUMS.

       Wage per hour is measured directly for hourly workers. For
       salaried workers, equal to salary / hours worked.
       ";

julia> printnotes(df)
Column: wage
────────────

Wage per hour in 2014 USD taken from ACS data provided by IPUMS.

Wage per hour is measured directly for hourly workers. For
salaried workers, equal to salary / hours worked.



julia> @note! df :wage = "Wage is capped at 99th percentile";

julia> printnotes(df)
Column: wage
────────────

Wage per hour in 2014 USD taken from ACS data provided by IPUMS.

Wage per hour is measured directly for hourly workers. For
salaried workers, equal to salary / hours worked.

Wage is capped at 99th percentile
```
"""
macro note!(df, args...)
    esc(addnote_helper(df, args...))
end


"""
    printlabels(df, [cols=All()]; unlabelled = true)

Pretty-print all labels in a data frame.

## Arguments

* `cols`: Optional argument to select columns to print. Can
  be any valid multi-column selector, such as `Not(...)`,
  `Between(...)`, or a regular expression.

* `unlabelled`: Keyword argument for whether to print
  the columns without user-defined labels. Deftaults to `true`.
  For column `col` without a user-defined label, `label(df, col)` returns
  the name of the column, `col`.

## Examples
```julia-repl
julia> df = DataFrame(wage = [12], age = [23]);

julia> @label! df :wage = "Hourly wage (2015 USD)";

julia> printlabels(df)
┌────────┬────────────────────────┐
│ Column │                  Label │
├────────┼────────────────────────┤
│   wage │ Hourly wage (2015 USD) │
│    age │                    age │
└────────┴────────────────────────┘

julia> printlabels(df, :wage)
┌────────┬────────────────────────┐
│ Column │                  Label │
├────────┼────────────────────────┤
│   wage │ Hourly wage (2015 USD) │
└────────┴────────────────────────┘

julia> printlabels(df; unlabelled = false)
┌────────┬────────────────────────┐
│ Column │                  Label │
├────────┼────────────────────────┤
│   wage │ Hourly wage (2015 USD) │
└────────┴────────────────────────┘

julia> printlabels(df, r"^wage")
┌────────┬────────────────────────┐
│ Column │                  Label │
├────────┼────────────────────────┤
│   wage │ Hourly wage (2015 USD) │
└────────┴────────────────────────┘
```

"""
function printlabels(df, cols=All(); unlabelled = true)
    cs = String[]
    ls = String[]
    for n in names(df, cols)
        lab = label(df, n)
        if unlabelled == true
            push!(cs, n)
            push!(ls, lab)
        else
            if n != lab
                push!(cs, n)
                push!(ls, lab)
            end
        end
    end
    t = DataFrame(Column = cs, Label = ls)
    pretty_table(t; show_subheader = false)
    return nothing
end

"""
    printnotes(df, cols = All(); unnoted = false)

Print the notes and labels in a data frame.

## Arguments
* `cols`: Optional argument to select columns to print. Can
  be any valid multi-column selector, such as `Not(...)`,
  `Between(...)`, or a regular expression.
* `unnoted`: Keyword argument for whether to print
  the columns without user-defined notes or labels.

For the purposes of printing, column labels are printed in
addition to notes. However column labels are not returned by
`note(df, col)`.

```
julia> df = DataFrame(wage = [12], age = [23]);

julia> @label! df :age = "Age (years)";

julia> @note! df :wage = "Derived from American Community Survey";

julia> @note! df :wage = "Missing values imputed as 0 wage";

julia> @label! df :wage = "Hourly wage (2015 USD)";

julia> printnotes(df)
Column: wage
────────────
Label: Hourly wage (2015 USD)
Derived from American Community Survey
Missing values imputed as 0 wage

Column: age
───────────
Label: Age (years)
```
"""
function printnotes(df, cols = All(); unnoted = false)
    nms = names(df, cols)
    for n in nms
        nt = note(df, n)
        lab = label(df, n)
        no_note = nt == ""
        no_lab = lab == n
        if unnoted == true
            printnote(n, nt, lab, no_note, no_lab)
        else
            if no_note == false || no_lab == false
                printnote(n, nt, lab, no_note, no_lab)
            end
        end
    end
    nothing
end

function printnote(n, nt, lab, no_note, no_lab)
    # "Column: " has 8 characters
    println("Column: $n")
    println(repeat("─", length(n) + 8))
    if no_lab == false
        println("Label: ", lab)
    end
    if no_note == false
        println(nt)
    end
    println()
end