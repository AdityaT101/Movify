class SqlQueries:
    Insert_movie_data_fact = (
        """INSERT into "movies_fact" ( "movie_Id" , "adult" , "budget" , "original_language" , "original_title" , "popularity" , "release_date" , "revenue" , "vote_average" , "vote_count" ) 
                	    select 
                    		distinct ( SA."id") as id,
                    		 SE."adult",
                    		 SE."budget",
                    		 SE."original_language", 
                    		 SE."original_title",
                    		 SA.pop_avg,
                    		 SE."release_date", 
                    		 SE."revenue", 
                    		 SE."vote_average", 
                    		 CAST ( SA.vote_avg AS INTEGER)
                    		from "movies_staging" as SE
                    		JOIN 
                    		( 
                    			  select
                    				"movies_staging"."id" , 
                    				"avg"("movies_staging"."popularity")  as pop_avg,
                    				"avg"("movies_staging"."vote_count")  as vote_avg
                    				from "movies_staging"
                    				group by "movies_staging"."id"  
                    		) as SA
                    		ON( SA."id" = SE."id");
         """ )




    remove_movie_data_fact = ("""
    DELETE from "movies_fact";
    """)

    
    #===============================================
    
    
    Insert_time_data_Dimension = ("""
         Insert into "Time_Dimension"( "Date_Id", "Date", "Day", "Week", "Month", "Year")
            select 
                	 DISTINCT( TO_CHAR( release_date, 'yyyyMMDD' )::integer ) AS time_key,
                	 release_date,
                	 EXTRACT(day FROM release_date) AS day,
                	 EXTRACT(week FROM release_date ) AS week,
                	 EXTRACT(month FROM release_date) AS month,
                	 EXTRACT(year FROM release_date) AS year
                	 from "Movies_Staging"
                	 where release_date IS NOT NULL
                	 Order by year, month, day;		
         """)

    
    Remove_time_data_Dimension = ("""
           DELETE from "Time_Dimension";
    """)
    
    
    #===============================================
    
    
    
    Genre_table_insert = ("""
        Insert into "Movie_Genre_Dimension"( "movie_Id", "Genre_Id","Genre_Name" )
            select "Movie_Genre_Staging"."movie_Id" , "Movie_Genre_Staging"."Genre_Id" , "Movie_Genre_Staging"."Genre_Name" 
            	from "Movies_Fact" 
            	INNER JOIN "Movie_Genre_Staging"
            	on "Movies_Fact"."movie_Id" = "Movie_Genre_Staging"."movie_Id"	
        """)

    
    Genre_table_delete = ("""
        DELETE from Movie_Genre_Dimension;
    """)
    
    
    
    #===============================================
    
    Insert_Cast_Data_Dimension = ("""
         Insert into "Cast_Dimension"( "movie_Id", "Cast_Id", "Cast_Name" )
            	select "Cast_Staging5"."movie_Id" , "Cast_Staging5"."Cast_Id" , "Cast_Staging5"."Cast_Name" 
            	from "Movies_Staging" INNER JOIN "Cast_Staging5"
            	on "Movies_Staging"."id"  = "Cast_Staging5"."movie_Id";
         """)

    
    
    Remove_Cast_Data_Dimension = ("""
    DELETE from Cast_Dimension;
    """)
    
    
    #===============================================
    
    Insert_Ratings_Data_Dimension = ("""
        Insert into "Ratings_Dimension"("movie_Id" , "userId" , "rating" )
            select "Ratings_Staging"."movieId" , "Ratings_Staging"."userId" , "Ratings_Staging"."rating" 
        		from "Ratings_Staging";	
        """)

    
    
    Remove_Ratings_Data_Dimension = ("""
    DELETE from Ratings_Dimension;
    """)
    
    
    
    #===============================================
    
    
    
    Insert_CPI_Data_Dimension = ("""
        Insert into "CPI_Dimension"("date" ,  "CPI" )
            select 
                DISTINCT (CPI_Staging.date),
                "CPI_Staging"."CPI"
        		    from "CPI_Staging" INNER JOIN "Movies_Fact" on "CPI_Staging"."date" = "Movies_Fact"."release_date"
                order by "CPI_Staging"."date"
        """)

    
    
    Remove_CPI_Data_Dimension = ("""
        DELETE from CPI_Dimension;
    """)
    
    
    
    #===============================================
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    