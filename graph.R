df <- read.csv("output.txt/part-00000",header=F,colClasses=c(NA,"POSIXct","factor"))
df$V4 <- 1
df2 <- ddply(df,.(V3),transform,CSum=cumsum(V4))
df2 <- df2[ order(df2$V2), ]
df2$total <- cumsum(df2$V4)
df2$frac <- df2$CSum/df2$total
ggplot() + geom_line(aes(x=df2$V2,y=df2$frac,color=df2$V3)) + ylim(c(0,0.33))


library(hexbin)
library(RColorBrewer)
rf <- colorRampPalette(brewer.pal(9,'Reds'))
df <- read.csv("projection.txt/part-00000",header=F)

png("X1X2.png")
plot(hexbin(data.frame(df$V1,df$V2)), colramp=rf, xlab="1er axe factoriel", ylab="2ème axe factoriel")
dev.off()

png("X1X3.png")
plot(hexbin(data.frame(df$V1,df$V3)), colramp=rf, xlab="1er axe factoriel", ylab="3ème axe factoriel")
dev.off()

png("X2X3.png")
plot(hexbin(data.frame(df$V2,df$V3)), colramp=rf, xlab="2ème axe factoriel", ylab="3ème axe factoriel")
dev.off()

