# simple-database

A simple in-memory columnar database.

Note: A very simple tokenizer implementation. Every token/keyword needs to be whitespace separated.

## Requirements
1. [jsoncpp](https://github.com/open-source-parsers/jsoncpp)

## Example Usage

### Create Query

```cpp
auto operators = sdb::ParseInputQuery(
    "create test_table with col1 int col2 double col3 string");

sdb::Table_Vec vec;
for (auto &op : operators) {
  op->AddData(std::move(vec));
  op->Execute();
  vec = op->GetData();
}
```

### Insert Query

```cpp

auto operators =
    sdb::ParseInputQuery("insert test_table values 42 2.5 lol");

sdb::Table_Vec vec;
for (auto &op : operators) {
  op->AddData(std::move(vec));
  op->Execute();
  vec = op->GetData();
}
```

### Update Query

```cpp

auto operators =
    sdb::ParseInputQuery("Update u1_table values name lol age 11");

sdb::Table_Vec vec;
for (auto &op : operators) {
  op->AddData(std::move(vec));
  op->Execute();
  vec = op->GetData();
}
```

### Select Query

```cpp
auto operators = sdb::ParseInputQuery(
    "Select * from simple_table Where ( "
    "( name == john ) OR ( age > 40 ) )");

sdb::Table_Vec vec;
for (auto &op : operators) {
  op->AddData(std::move(vec));
  op->Execute();
  vec = op->GetData();
}
```

### Join Query

```cpp
auto operators = sdb::ParseInputQuery(
    "select * from jl_table join jr_table on ( "
    "jl_table.gid == jr_table.id ) where ( ( jl_table.age > 40 ) and ( "
    "jr_table.id == 1 ) )");

sdb::Table_Vec vec;
for (auto &op : operators) {
  op->AddData(std::move(vec));
  op->Execute();
  vec = op->GetData();
}
```

