# DataFramesMeta.jl tutorial

This is a port of the HarvardX series PH525x Genomics class tutorial on dplyr. View the original [here](https://genomicsclass.github.io/book/pages/dplyr_tutorial.html) and the source [here](https://github.com/genomicsclass/labs/blob/master/intro/dplyr_tutorial.Rmd).

## What is DataFramesMeta.jl?

DataFramesMeta.jl is a Julia package to transform and summarize tabular data. It provides a more convenient syntax to work with DataFrames from [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl). For a deeper explanation of DataFramesMeta.jl, see the [documentation](https://github.com/JuliaData/DataFramesMeta.jl). 

DataFramesMeta.jl is heavily inspired by R's `dplyr`. If you are familiar with `dplyr` this guide should get you up to speed with DataFramesMeta.jl. 

## Why Is It Useful?

Like dplyr, the DataFramesMeta.jl package contains a set of macros (or "verbs") that perform common data manipulation operations such as filtering for rows, selecting specific columns, re-ordering rows, adding new columns and summarizing data. 

In addition, DataFramesMeta.jl contains a useful operation `@combine` to perform another common task which is the "split-apply-combine" concept. We will discuss that in a little bit. 

## How Does It Compare To Using Base Functions in Julia and in DataFrames.jl?

If you are familiar with Julia, you are probably familiar with base Julia functions such  `map`, and `broadcast` (akin to `*apply` in R). These functions are convenient to use, but are designed to work with `Vector`s, not tabular data.

DataFrames provides the functions `select`, `transform`, and more to work with data frames. Unlike `map` and `broadcast`, these functions are designed to work with tabular data, but have a complicated syntax. 

DataFramesMeta.jl provides a convenient syntax for working with the vectors in a `DataFrame` so that you get all the convenience of Base Julia and DataFrames combined. 

## How Do I Get DataFramesMeta.jl? 

To install DataFramesMeta.jl:

```julia
import Pkg
Pkg.activate(; temp=true) # activate a temprary environment for this tutorial
Pkg.add("DataFramesMeta");
```

To load DataFramesMeta.jl

```@example 1
using DataFramesMeta
```

For this tutorial, we will install some additional packages as well. 

```julia dplyr
Pkg.add(["CSV", "HTTP"])
```

Now we load them. We also load the Statistics standard library, which is shipped with Julia, so does not need to be installed.

```@example 1
using CSV, HTTP, Statistics
```

We will use [CSV.jl](https://csv.juliadata.org/stable/) and [HTTP.jl](https://juliaweb.github.io/HTTP.jl/stable/) for downloading our dataset from the internet.

# Data: Mammals Sleep

The `msleep` (mammals sleep) data set contains the sleep times and weights for a set of mammals and is available in the dagdata repository on github. This data set contains 83 rows and 11 variables.  

We can loads the data directly into a DataFrame from the `url`. 

```@example 1
url = "https://raw.githubusercontent.com/genomicsclass/dagdata/master/inst/extdata/msleep_ggplot2.csv"
msleep = CSV.read(HTTP.get(url).body, DataFrame; missingstring="NA")
```

The columns (in order) correspond to the following: 

column name | Description
--- | ---
`:name` | common name
`:genus` | taxonomic rank
`:vore` | carnivore, omnivore or herbivore?
`:order` | taxonomic rank
`:conservation` | the conservation status of the mammal
`:sleep_total` | total amount of sleep, in hours
`:sleep_rem` | rem sleep, in hours
`:sleep_cycle` | length of sleep cycle, in hours
`:awake` | amount of time spent awake, in hours
`:brainwt` | brain weight in kilograms
`:bodywt` | body weight in kilograms


# Important DataFramesMeta.jl Verbs To Remember

dplyr verbs | Description
--- | ---
`@select` | select columns 
`@subset` | filter rows
`@orderby` | re-order or arrange rows
`@transform` | create new columns
`@combine` | summarise values
`groupby` | allows for group operations in the "split-apply-combine" concept

DataFramesMeta.jl also provides `@rselect`, `@rsubset`, `@rorderby`, and `@rtransform` for operations which act row-wise. We will expore the distinction between column-wise and row-wise transformations later in this turorial. 

# DataFramesMeta.jl Verbs In Action

Two of the most basic functions are `@select` and `@subset`, which selects columns and filters rows respectively. To reference columns, use the `Symbol` of the column name, i.e. `:name` refers to the column `msleep.name`.

## Selecting Columns Using `@select`

Select a set of columns: the `:name` and the `:sleep_total` columns. 

```@example 1
sleepData = @select msleep :name :sleep_total
```

To select all the columns *except* a specific column, use the `Not` function for inverse selection. We preface the `Not` with `$` because it does not reference a column directly as a `Symbol`.

```@example 1
@select msleep $(Not(:name))
```

To select a range of columns by name, use the `Between` operator:

```@example 1
@select msleep $(Between(:name, :order))
```

To select all columns that start with the character string `"sl"` use regular expressions. 

```@example 1
@select msleep $(r"^sl")
```

Regular expressions are powerful, but can be difficult for new users to understand. Here are some quick tips.  

1. `r"^abc"` = Starts with `"abc"`
2. `r"abc$"` = Ends with `"abc"`
3. `r"abc"` = Contains `"abc"` anywhere. 

## Selecting Rows Using `@subset` and `@rsubset`

Filter the rows for mammals that sleep a total of more than 16 hours. 

```@example 1
@subset msleep :sleep_total .>= 16
```

In the above expression, the `.>=` means we "broadcast" the `>=` comparison across the whole column. We can use a simpler syntax, `@rsubset` which automatically broadcasts all operations.

```@example 1
@rsubset msleep :sleep_total > 16
```

Subset the rows for mammals that sleep a total of more than 16 hours *and* have a body weight of greater than 1 kilogram. For this we put multiple operations on separate lines in a single block. 

```@example 1
@rsubset msleep begin 
    :sleep_total >= 16 
    :bodywt >= 1
end
```

Filter the rows for mammals in the Perissodactyla and Primates taxonomic order

```@example 1
@rsubset msleep :order in ["Perissodactyla", "Primates"]
```

You can use the boolean operators (e.g. >, <, >=, <=, !=, in) to create the logical tests. 

# Chain.jl

Before we go any further, let's introduce the `@chain` macro from [Chain.jl](https://github.com/jkrumbiegel/Chain.jl), which is re-exported by DataFramesMeta.jl. `@chain` allows for you to pipe the output from one operation into the input of another operation.  The idea of piping is to read the functions from left to right. The syntax and design of `@chain` is very similar to `%>%` which users of dplyr are familiar with.

To show it's usefulness, let's use `@select` and `@rsubset` one after the other. 

```
msleep_1 = @select msleep :name :sleep_total
msleep_2 = @rsubset msleep_1 :sleep_total > 16
```

Now in this case, we will pipe the msleep data frame to the function that will select two columns (name and sleep\_total) and then pipe the new data frame to the `@rsubset` opertaion. This method involves awkwardly creating and naming temporary data frames. This can be avoided with `@chain`.

```@example 1
@chain msleep begin 
  @select :name :sleep_total
  @rsubset :sleep_total > 16
end
```

You will soon see how useful the `@chain` macro is when we start to combine many functions.  

# Back To dplyr Verbs In Action

Now that you know about the `@chain` macro, we will use it throughout the rest of this tutorial. 

## Arrange Or Re-order Rows Using `@orderby`

To arrange (or re-order) rows by a particular column, such as the taxonomic order, list the name of the column you want to arrange the rows by:

```@example 1
@orderby msleep :order
```

Now we will select three columns from msleep, arrange the rows by the taxonomic order and then arrange the rows by sleep\_total. Finally, keep the first 10 rows of the data frame.

```@example 1
@chain msleep begin 
    @select :name :order :sleep_total
    @orderby :order :sleep_total
    first(10)
end
```

Same as above, except here we filter the rows for mammals that sleep for 16 or more hours, instead of showing the head of the final data frame:

```@example 1
@chain msleep begin 
    @select :name :order :sleep_total
    @orderby :order :sleep_total 
    @rsubset :sleep_total > 16
end
```

Something slightly more complicated: same as above, except arrange the rows in the `:sleep_total` column in a descending order. For this, use the function `sortperm` with the keyword argument `rev=true`.

```@example 1
@chain msleep begin 
    @select :name :order :sleep_total
    @orderby begin 
        :order 
        sortperm(:sleep_total, rev=true)
    end 
    @rsubset :sleep_total >= 16
end
```

## Create New Columns Using `@transform` and `@rtransform`

The `@transform` macro will add new columns to the data frame. Like with other macros, use `@rtransform` to operate row-wise. Create a new column called `:rem_proportion`, which is the ratio of rem sleep to total amount of sleep. 

```@example 1
@rtransform msleep :rem_proportion = :sleep_rem / :sleep_total
```

You can many new columns using `@transform` by placing multiple operations in a block.

```@example 1
@rtransform msleep begin 
    :rem_proportion = :sleep_rem / :sleep_total 
    :bodywt_grams = :bodywt * 1000
end
```

## Create summaries of the data frame using `@combine`

The `@combine` macro will create summary statistics for a given column in the data frame, such as finding the mean. For example, to compute the average number of hours of sleep, apply the `mean` function to the column `:sleep_total` and call the summary value `:avg_sleep`. 

```@example 1
@chain msleep @combine :avg_sleep = mean(:sleep_total)
```

There are many other summary statistics you could consider such `std`, `minimum`, `maximum`, `median`, `sum`, `length` (returns the length of vector), `first` (returns first value in vector), and `last` (returns last value in vector).

```@example 1
@combine msleep begin 
    avg_sleep = mean(:sleep_total) 
    min_sleep = minimum(:sleep_total)
    max_sleep = maximum(:sleep_total)
    total = length(:sleep_total)
end
```

## Group operations using `group`

The `groupby` verb is an important function in DataFrames.jl (it does not live in DataFramesMeta.jl). As we mentioned before it's related to concept of "split-apply-combine". We literally want to split the data frame by some variable (e.g. taxonomic order), apply a function to the individual data frames and then combine the output.   

Let's do that: split the `msleep` data frame by the taxonomic order, then ask for the same summary statistics as above. We expect a set of summary statistics for each taxonomic order. 

```@example 1
@chain msleep begin 
    groupby(:order)
    @combine begin 
        avg_sleep = mean(:sleep_total)
        min_sleep = minimum(:sleep_total)
        max_sleep = maximum(:sleep_total)
        total = length(:sleep_total)
    end
end
```

Split-apply-combine can also be used with `@transform` to add new variables to a data frame by performing operations by group. For instance, we can de-mean the total hours of sleep of an anymal relative to other animals in the same genus. 

```@example 1
@chain msleep begin 
    groupby(:order)
    @transform sleep_genus = :sleep_total .- mean(:sleep_total)
end
```

This short tutorial only touches on the wide array of features in Julia, DataFrames.jl, and DataFramesMeta.jl. Read the [full documentation](https://github.com/JuliaData/DataFramesMeta.jl) for more information.