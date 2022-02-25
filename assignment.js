
// Q1. General Queries //

1) db.movies.aggregate([
      {$unwind:"$genres"},
      {$group:{_id:"$genres",avgDuration:{$avg:"$runtime"}}},
      {$sort:{"avgDuration":-1}},
      {$limit:5}
   ])

 2) db.movies.aggregate([
      {$match:{countries:"UK"}},
      {$project: {countries: {$filter: {input:"$countries",as: "item",
       cond: {$ne: ["$$item", "UK"]}}}}},
      {$unwind:"$countries"},
      {$group:{_id:"$countries",numMovies:{$sum:1}}},
      {$match:{numMovies:{$gte:10}}}
    ])


// Q2. Movies Related to Sports //

  1) db.movies.createIndex({"title":"text","genres":"text","plot":"text","tomato.consensus":"text"})

     db.movies.find(
       {$text:{$search:"sport football hockey NFL basketball soccer tennis cricket olympics rugby"}},
       {score:{$meta:"textScore"}}).sort({score:{$meta:"textScore"}}).count()

     db.movies.aggregate([
       {$match:{$text:{$search:"sport football hockey NFL basketball soccer tennis cricket olympics rugby"}}},
       {$group:{_id:"sportMovies",count:{$sum:1}}}
     ])

  2) db.movies.aggregate([
      {$match:{$text:{$search:"sport football hockey NFL basketball soccer tennis cricket olympics rugby"}}},
      {$sort:{"imdb.rating":-1}},
      {$project:{title:1,year:1,rating:"$imdb.rating",votes:"$imdb.votes",_id:0}},
      {$limit:3}
     ])


// Q3. Rating and Recommending Movies //

  1) db.movies.updateMany({},
        [{$addFields: {
          tomatoRating: {$multiply: [{$divide:[{$sum:["$tomato.meter","$tomato.userMeter"]}, 200]}, 5]},
          metaRating: {$multiply: [{$divide:["$metacritic",100]}, 5]},
          imdbRating: {$multiply:[{$divide:["$imdb.rating",10]}, 5]}}},
        {$addFields:{
          tomatoRating:{$cond:
                        {if: {$eq: ["$tomatoRating", 0]},
                 then: null,
                 else: "$tomatoRating"}},
         metaRating:{$cond:
                       {if: {$eq: ["$metaRating", 0]},
                then: null,
                else: "$metaRating"}},
         imdbRating:{$cond:
                      {if: {$eq: ["$imdbRating", 0]},
               then: null,
               else: "$imdbRating"}} }},
        {$addFields:{myRating:{$round:[{$avg: ["$tomatoRating", "$metaRating", "$imdbRating"]}, 1]}}},
        {$unset:["tomatoRating","metaRating","imdbRating"]}
      ])

  2) db.movies.aggregate([
        {$match: {
            $and:[
              {$or:[
                  { year: {$gte: 1960, $lt: 1970} },
                  { year: {$gte: 1980, $lt:1990} }
                ]},
                {genres:{$in: ["Drama", "Western", "Crime"]}},
                {"awards.wins":{$gt:2}},
                {myRating:{$gte:4}},
                {countries:{$in: ["USA","UK"]}},
              ]
            }
          },
          {$project:{title:1,myRating:1,_id:0}},
          {$limit:3}
          ])
