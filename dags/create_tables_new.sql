DROP TABLE IF EXISTS public.Movies_Fact;
DROP TABLE IF EXISTS public.Movies_Staging; 
DROP TABLE IF EXISTS public.Cast_Staging; 
DROP TABLE IF EXISTS public.Cast_Dimension; 
DROP TABLE IF EXISTS public.Movie_Genre_Staging;
DROP TABLE IF EXISTS public.Movie_Genre_Dimension;
DROP TABLE IF EXISTS public.Ratings_Dimension;
DROP TABLE IF EXISTS public.Ratings_Staging;
DROP TABLE IF EXISTS public.Time_Dimension;
DROP TABLE IF EXISTS public.CPI_Dimension;
DROP TABLE IF EXISTS public.CPI_Staging;
 
 
 
 
 
 
CREATE TABLE "public"."CPI_Staging" (
  "date" date,
  "CPI" float4
);


CREATE TABLE "public"."CPI_Dimension" (
  "CPI_Id" int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY(1,1),
  "date" date,
  "CPI" float4
);
 
 
 
 
 
 
 
 
 
CREATE TABLE "public"."Movies_Fact" (
  "movie_Id" int8 NOT NULL,
  "adult" bool,
  "budget" int8,
  "original_language" varchar(10),
  "original_title" varchar(1000),
  "popularity" float4,
  "release_date" date,
  "revenue" int8,
  "vote_average" float4,
  "vote_count" int4,
  CONSTRAINT "Movies_Fact_pkey" PRIMARY KEY ("movie_Id")
);




CREATE TABLE "public"."movies_staging" (
  "adult" bool,
  "belongs_to_collection" varchar(1000),
  "budget" int8,
  "genres" varchar(1000) ,
  "homepage" varchar(1000) ,
  "id" int8,
  "imdb_id" varchar(32) ,
  "original_language" varchar(10) ,
  "original_title" varchar(1000) ,
  "overview" varchar(5000) ,
  "popularity" float4,
  "poster_path" varchar(1000) ,
  "production_companies" varchar(2000) ,
  "production_countries" varchar(2000) ,
  "release_date" date,
  "revenue" int8,
  "runtime" float4,
  "spoken_languages" varchar(1000) ,
  "status" varchar(50) ,
  "tagline" varchar(1000) ,
  "title" varchar(1000) ,
  "video" bool,
  "vote_average" float4,
  "vote_count" int4
);

 
 
 
 
 
 
 

CREATE TABLE "public"."Cast_Staging5" (
  "Movie_Id" int4,
  "Cast_Id" int4,
  "Cast_Name" varchar(255)
); 




CREATE TABLE "public"."Cast_Dimension" (
  "Movie_Cast_Id" int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY(1,1),
  "movie_Id" int4,
  "Cast_Id" int4,
  "Cast_Name" varchar(255),
  CONSTRAINT "Cast_Dimension_pkey" PRIMARY KEY ("Movie_Cast_Id")
);


 







CREATE TABLE "public"."Movie_Genre_Staging" (
  "movie_Id" int4,
  "Genre_Id" int4,
  "Genre_Name" varchar(255)
);



CREATE TABLE "public"."Movie_Genre_Dimension" (
  "Movie_Genre_Id" int4 NOT NULL GENERATED ALWAYS AS IDENTITY(1,1),
  "movie_Id" int4,
  "Genre_Id" int4,
  "Genre_Name" varchar(255),
  CONSTRAINT "Movie_Genre_Dimension_pkey" PRIMARY KEY ("Movie_Genre_Id")
);

 








CREATE TABLE "public"."Ratings_Dimension" (
  "Movie_Rating_Id" int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY (1,1),
  "movie_Id" int4,
  "userId" int4,
  "rating" float4,
  CONSTRAINT "Ratings_Dimension_pkey" PRIMARY KEY ("Movie_Rating_Id")
);



CREATE TABLE "public"."Ratings_Staging" (
  "userId" int4,
  "movieId" int4,
  "rating" float4,
  "timestamp" int8
);








CREATE TABLE "public"."Time_Dimension" (
  "Date_Id" int4 NOT NULL,
  "Date" date,
  "Day" int4,
  "Week" int4,
  "Month" int4,
  "Year" int4,
  CONSTRAINT "Time_Dimension_pkey" PRIMARY KEY ("Date_Id")
);







