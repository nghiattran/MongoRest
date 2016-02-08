# Data types

| Type                    | Number | Alias                 | Notes       |
|-------------------------|--------|-----------------------|-------------|
| Double                  | 1      | “double”              |             |
| String                  | 2      | “string”              |             |
| Object                  | 3      | “object”              |             |
| Array                   | 4      | “array”               |             |
| Binary data             | 5      | “binData”             |             |
| Undefined               | 6      | “undefined”           | Deprecated. |
| Object id               | 7      | “objectId”            |             |
| Boolean                 | 8      | “bool”                |             |
| Date                    | 9      | “date”                |             |
| Null                    | 10     | “null”                |             |
| Regular Expression      | 11     | “regex”               |             |
| DBPointer               | 12     | “dbPointer”           |             |
| JavaScript              | 13     | “javascript”          |             |
| Symbol                  | 14     | “symbol”              |             |
| JavaScript (with scope) | 15     | “javascriptWithScope” |             |
| 32-bit integer          | 16     | “int”                 |             |
| Timestamp               | 17     | “timestamp”           |             |
| 64-bit integer          | 18     | “long”                |             |
| Min key                 | -1     | “minKey”              |             |
| Max key                 | 127    | “maxKey”              |             |

# ObjectId format

ObjectIds use 12 bytes of storage, which gives them a string representation that is 24
hexadecimal digits: 2 digits for each byte. 

| 0  1  2  3 | 4 5 6 7 |  8 9 |  10   11  |
|-----------------------------------------|
|  Timestamp | Machine |  PID | Increment |

# Array modifiers

### Adding

##### Adding one with $push

````
    {
        '$push': {
            'array': {
                array_element
            }
        }
    }
````

##### Adding many with $each

````
    {
        '$push': {
            'array': {
                '$each': [array_element,array_element,array_element],
            }
        }
    }
````

##### Keep array length with $slice

`$slice` value must be negative. Keeping the last pushed items

````
    {
        '$push': {
            'array': {
                '$each': [array_element,array_element,array_element],
                '$slice' : -10
            }
        }
    }
````
##### Sorting before slice

Possible sorts (`value`) are 1 (for ascending) and −1 (for descending).

````
    {
        '$push': {
            'array': {
                '$each': [array_element,array_element,array_element],
                '$slice': -10,
                '$sort': {array_value: value}
            }
        }
    }
````

###### P/S: 
Note that you must include "$each"; you cannot just "$slice" or "$sort" an array with "$push".

##### Prevent duplications with $ne and $addToSet

```
    {
        "$addToSet" : {
            "emails" : "joe@gmail.com"
        }
    }
```

`$addToSet` can be used with `$each`

```
    {
        "$addToSet" : {
            "emails" : {
                "$each" :["joe@php.net", "joe@example.com", "joe@python.org"]
            }
        }
    }
```

##### Removing elements with $pop, $pull

`$pop` treats array as a queue. `key` takes 1 (poping from the end of array) and -1.

```
     {"$pop" : {"key" : 1}}
```

`$pull` remove a specific item(s) in array

Pulling one:

```
    {
        "$pull" : {
            "todo" : "laundry"
        }
    }
```

Pulling many:

```
    {
        "$pull" : {
            "todo" : [
                 "dishes",
                 "dry cleaning"
            ]
        }
    }
```

##### Positional array modifications

Increment `votes` of the `comments[0]` by 1.

```
    {"$inc" : {"comments.0.votes" : 1}}
```

Find John in `comments.author` and change it to Jim.

```
    db.blog.update(
        {
            "comments.author" : "John"
        },  
        {
            "$set" : {
                "comments.$.author" : "Jim"
            }
        }
    )
```

This only change the first match.

##### Modifier speed
Typically, documents are stored next to each other, so if your documents get 
bigger, they will be moving to the end. Moving is slow
Beware of "paddingFactor" in MongoDB. If you change the size of a document,  
it will leave a hole in memory which will lead to memory inefficiency.

MongoDB is currently not great at reusing free spaces left by moving.
 
TODO: Compacting data and schema design solutions


# Upsert

If no document is found that matches the update criteria, a new document will
 be created by combining the criteria and updated documents.
 
Use "$setOnInsert` to seed document when it is created and not will not updated.

```
    db.users.update(
        {},
        {
            "$setOnInsert" : {
                "createdAt" : new Date()
            }
        },
        true)
```

# Write Concern

Write concern is a client setting used to describe how safely a write should 
be stored before the application continues. By default, inserts, removes, and
 updates wait for a database response—did the write succeed or not?—before continuing.
 

# Queries

### Conditional queries

"$lt", "$lte", "$gt", "$ne", "$gte", "$mod", "$not"

### OR queries

OR condition for a single key: `$in` or `$nin` (not in)

```
    db.users.find(
        {
            "user_id" : {
                "$in" : [12345, "joe"]
            }
        })
```

OR condition with '$or'

```
    db.raffle.find(
        {
            "$or" : [{"ticket_no" : 725}, {"winner" : true}]
        })
        
    db.raffle.find(
        {
            "$or" : [
                {
                    "ticket_no" : {"$in" : [725, 542, 390]}
                },{
                    "winner" : true
                }]
        })
```

### Regular Expressions

Mongo uses  Perl Compatible Regular Expression to match regex. Should check 
with js shell before using in queries.

Find case-insensitive 

```
    db.users.find({"name" : /joe/i})
    return joe and Joe
    
    db.users.find({"name" : /joey?/i})
    return joe and Joe and joey and Joey
```

### Querying Arrays

Query an item in array

```
    db.food.find(
        {
            "fruit" : "banana"
        })
```

Query many items in array

```
    db.food.find(
        {
            fruit : {
                $all : ["apple", "banana"]
            }
        })
```

Query exact match

```
    db.food.find(
        {
            "fruit" : ["apple", "banana", "peach"]
        })
```

Query with specific item value. `fruit[2]` == "peach"

```
    db.food.find(
        {
            "fruit.2" : "peach"
        })
```

### Array and range query interactions

For example, we have:

```
    {"x" : 5}
    {"x" : 15}
    {"x" : 25}
    {"x" : [5, 25]}
```

##### Query with $lt and $gt

Query

```
    db.test.find(
        {
            "x" : {
                "$gt" : 10, "$lt" : 20
            }
        })
```

Return

```
    {"x" : 15}
    {"x" : [5, 25]}
```

Becasue 5 < 20 and 25 > 10.

##### Query with "$elemMatch"

Query

```
    db.test.find(
        {
            "x" : {
                "$elemMatch" : {
                    "$gt" : 10, "$lt" : 20
                }
            }
        })
```

Return

```
    no results
```

Because `$elemMatch` won’t match non-array elements.

##### Query with min() and max()

Query

```
    db.test.find(
        {
            "x" : {
                "$gt" : 10, "$lt" : 20
            }
        }).min({"x" : 10}).max({"x" : 20})
```

Return

```
    {"x" : 15}
```

### Querying on Embedded Documents

##### Querying for the whole document

The following is order sensitive

```
    db.people.find(
        {
            "name" : {
                "first" : "Joe", "last" : "Schmoe"
            }
        })
```

##### Querying for its individual key/value pairs

```
    db.people.find(
        {
            "name.first" : "Joe", 
            "name.last" : "Schmoe"
        })
```

##### Querying in nested embedded documents

Example: want to find comments by Joe that were scored at least a 5

```
{
    "content" : "...",
    "comments" : [
        {
            "author" : "joe",
            "score" : 3,
            "comment" : "nice post"
        },
        {
            "author" : "mary",
            "score" : 6,
            "comment" : "terrible post"
        }
    ]
}
```

These queries won't work
```
    // Match exact document
    db.blog.find(
        {
            "comments" : {
                "author" : "joe",
                "score" : {
                    "$gte" : 5}
                }
            }
        })
        
    // Return the both ??? why both ???
    db.blog.find(
        {
            "comments.author" : "joe",
            "comments.score" : {
                "$gte" :5
            }
        })
```


```
    db.blog.find(
        {
            "comments" : {
                "$elemMatch" : {
                    "author" : "joe",
                    "score" : {
                        "$gte" : 5
                    }
                }
            }
        })
```

# $where Queries

Use aggregation instead

# Server-Side Scripting

```js
    func = "function() { print('Hello, "+name+"!'); }"
```

Might turn to 

```js
    func = "function() { print('Hello, '); db.dropDatabase(); print('!'); }"
```

Which will run `db.dropDatabase()` and you lose entire database.
To prevent this, use scope to pass in a name.

```python
    func = pymongo.code.Code(
        "function() {
            print('Hello, '+username+'!'); 
        }",{
            "username": name
        })
```

# Comparison order

1. Minimum value
2. null
3. Numbers (integers, longs, doubles)
4. Strings
5. Object/document
6. Array
7. Binary data
8. Object ID
9. Boolean
10. Date
11. Timestamp
12. Regular expression
13. Maximum value

# Avoiding Large Skips

Skip is slow and should be avoided

This is a bad implementation. Never do it.

```
    var page1 = db.foo.find(criteria).limit(100)
    var page2 = db.foo.find(criteria).skip(100).limit(100)
    var page3 = db.foo.find(criteria).skip(200).limit(100)
```

Using conditional query instead. For example.

```
    var page1 = db.foo.find().sort({"date" : -1}).limit(100)
    var latest = page1[100]
    
    // Query the next 100 items by querying based on date 
    var page2 = db.foo.find({"date" : {"$gt" : latest.date}}).sort({"date" : -1}).limit(100)
```

# Getting Consistent Results

It is very important to know that if you update your documents, chances are 
they will get bigger and no longer fit to the space they were before. MongoDB
 will move those documents to the end of the collection, so that in some 
 cases for program will run into a loop in which it tries to update the 
 updated objects.

You might want to use 

```
    db.foo.find().snapshot()
```

for updating objects once. Snapshotting makes queries slower.

Inconsistencies arise only when the collection changes under a cursor while 
it is waiting to get another batch of results.