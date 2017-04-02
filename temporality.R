## generalized temporality

## load packages
library(digest)

## begin function
temporize <- function(newdata,
                      existdata,
                      identifier,
                      track_values,
                      valid_from_col = "valid_from",
                      valid_to_col = "valid_to",
                      curr_date = as.Date(Sys.Date()),
                      fut_date = as.Date("2099-12-31"),
                      hash_method = "md5") {
        
        ## define other columns to update non-tracked values
        non_track <- names(existdata)[!(names(existdata) %in% 
                                                c(identifier, 
                                                  track_values, 
                                                  valid_from_col, 
                                                  valid_to_col))]
        
        ## apply hash to new data
        for (i in 1:nrow(newdata)) {
                newdata$hash[i] <- digest(sapply(newdata[i, c(identifier, track_values)], 
                                                 as.character), 
                                          algo = hash_method, 
                                          serialize = TRUE,
                                          ascii = TRUE,
                                          seed = 13)
        }
        
        ## apply hash to existing data
        for (j in 1:nrow(existdata)) {
                existdata$hash[j] <- digest(sapply(existdata[j, c(identifier, track_values)],
                                                   as.character), 
                                            algo = hash_method, 
                                            serialize = TRUE,
                                            ascii = TRUE,
                                            seed = 13)
        }
        
        ## update records where tracked values have not changed
        for (k in non_track) {
                existdata[existdata$hash %in% newdata$hash, 
                          k] <- newdata[match(existdata$hash[existdata$hash %in% 
                                                                     newdata$hash], 
                                              newdata$hash), k]
        }
        
        ## update valid to date where record is not in new data
        existdata$valid_to[!(existdata$hash %in% newdata$hash)] <- curr_date
        
        ## apply valid from and to dates to new data
        newdata$valid_from <- curr_date
        newdata$valid_to <- fut_date
        
        ## rename to match target columns
        names(newdata)[names(newdata) == "valid_from"] <- valid_from_col
        names(newdata)[names(newdata) == "valid_to"] <- valid_to_col
        
        ## add new records to existing data set
        existdata <- rbind(existdata, newdata[!(newdata$hash %in% existdata$hash), ])
        
        ## drop hash column
        existdata <- subset(existdata, select = -hash)
        
        ## return updated data set
        return(existdata)
        
}
