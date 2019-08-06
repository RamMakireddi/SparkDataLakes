## Goal
    The goal of this project is the build DWH analytical system in AWS cloud environment to analyze song play data for the music streaming startup, Sparkify.
    
## Target Users
    The data analysts and other analytical teams in Sparkify
    
## Source
    The source data is available in S3 in form of JSON files
    
## Target
    The target data will be in S3 modeled in STAR schema for easy and effective data analysis
    
## Design Approach
   ### ELT(Extract Load Transform)
       -> ELT approach is considered to improve ETL performance. 
       -> Extract json files from S3, process using Spark 
       -> Data is processed and transformed into Dimensions and Facts using Spark then loaded back into S3 for data analysis. The data is modeled dimensionally using STAR schema approach.
     
   ### STAR Schema
       Fact - songplays
       Dimensions: Time, User, Song & Artist
       

