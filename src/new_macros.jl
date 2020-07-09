function make_vec_to_fun(kw)
    output = kw.args[1]
    membernames = Dict{Any, Symbol}()
    funname = gensym()
    body = DataFramesMeta.replace_syms!(kw.args[2], membernames)
    
    # inputs = broadcast(s -> s.args[1], keys(membernames))

    t = quote
        $(Expr(:vect, keys(membernames)...)) => function $funname($(values(membernames)...))
            $body 
        end => $(QuoteNode(output))
    end
end

function transform_helper2(x, args...)

    t = [make_vec_to_fun(arg) for arg in args]

    quote 
        $DataFrames.transform($x, $(t...))
    end
end

macro transform2(x, args...)
    esc(transform_helper2(x, args...))
end

function based_on_helper2(x, args...)

    t = [make_vec_to_fun(arg) for arg in args]

    quote 
        $DataFrames.combine($x, $(t...))
    end
end

macro based_on2(x, args...)
    esc(based_on_helper2(x, args...))
end

function select_helper2(x, args...)
    t = [make_vec_to_fun(arg) for arg in args]

    quote 
        $DataFrames.select($x, $(t...))
    end
end

macro select2(x, args...)
    t = map(DataFramesMeta.expandargs, args)

    esc(select_helper2(x, t...))
end