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
    t = map(args) do e
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
    label(df, args...)

Shorthand for `label!(df, ...)`
"""
macro addlabel(df, args...)
    esc(addlabel_helper(df, args...))
end

function addnote_helper(df, args...)
    x, exprs, outer_flags, kw = get_df_args_kwargs(df, args...; wrap_byrow = false)
    x_sym = gensym()
    t = map(args) do e
        lhs, rhs = get_lhs_rhs(e)
        :($note!($x_sym, $lhs, $rhs; append = true))
    end
    labblock = Expr(:block, t...)
    quote
        $x_sym = $x
        $labblock
        $x_sym
    end
end

macro addnote(df, args...)
    esc(addnote_helper(df, args...))
end

function printlabels(df)
    d = colmetadata(df)
    t = DataFrame(Column = names(df))
    t.Label = labels(df)
    pretty_table(t; show_subheader = false)
    return nothing
end

function printnotes(df)
    L = maximum(length.(names(df))) + 8
    for n in names(df)
        println("Column: $n")
        println(repeat("=", L))
        nt = note(df, n)
        println(nt)
        println()
    end
end