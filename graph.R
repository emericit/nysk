library(ggplot2)

library(plyr)
df <- read.csv("output_LSA.txt/part-00000",header=F,colClasses=c(NA,"POSIXct","factor"))
df <- subset(df,df$V3==0 | df$V3==4 | df$V3 ==7 | df$V3==8)
df$V4 <- 1
df2 <- ddply(df,.(V3),transform,CSum=cumsum(V4))
df2 <- df2[ order(df2$V2), ]
df2$total <- cumsum(df2$V4)
df2$frac <- df2$CSum/df2$total
df2 <- rename(df2, c("V2"="Date", "V3"="Classe", "CSum"="Nombre total d'articles publiés"))
revalue(df2$Classe, c("0" = "Libération sous caution", "4" = "Candidature au FMI", "7" = "Élections françaises", "8" ="Déroulé judiciaire")) -> df2$Classe
ggplot() + geom_line(aes(x=df2$Date,y=df2$"Nombre total d'articles publiés",color=df2$Classe)) + xlab("Date") + ylab("Nombre total d'articles publiés")


library(hexbin)
library(RColorBrewer)
rf <- colorRampPalette(brewer.pal(9,'Reds'))
df <- read.csv("projection_LSA.txt/part-00000",header=F)

png("X1X2.png")
plot(hexbin(data.frame(df$V1,df$V2)), colramp=rf, xlab="1er axe factoriel", ylab="2ème axe factoriel")
dev.off()

png("X1X3.png")
plot(hexbin(data.frame(df$V1,df$V3)), colramp=rf, xlab="1er axe factoriel", ylab="3ème axe factoriel")
dev.off()

png("X2X3.png")
plot(hexbin(data.frame(df$V2,df$V3)), colramp=rf, xlab="2ème axe factoriel", ylab="3ème axe factoriel")
dev.off()

df <- read.csv("projection_W2V.txt/part-00000",header=F)
cl <- read.csv("classes_W2V.txt/part-00000",header=F)
df$Class <- as.factor(cl$V1)
revalue(df$Class, c("0" = "Politique américaine", "1" = "Les accusations", "2" = "Christine Lagarde", "3" = "classe 4", "4" = "ADN", "5" = "Libération sous caution", "6" = "Actualité boursière", "7" = "Logement", "8" = "classe 9", "9" = "Déroulement judiciaire")) -> df$Class
ggplot(df, aes(x=df$V1,y=df$V2,color=df$Class)) + geom_point() + xlab("1er axe factoriel") + ylab("2ème axe factoriel")
ggsave("classes_in_space_W2V.png")


df <- read.csv("output_W2V.txt/part-00000",header=F,colClasses=c(NA,"POSIXct","factor"))
df <- subset(df,df$V3==10 | df$V3==2 | df$V3 ==4 | df$V3==5 | df$V3==7  | df$V3==9)
revalue(df$V3, c("0" = "Politique américaine", "1" = "Les accusations", "2" = "Christine Lagarde", "3" = "classe 4", "4" = "ADN", "5" = "Libération sous caution", "6" = "Actualité boursière", "7" = "Logement", "8" = "classe 9", "9" = "Déroulement judiciaire")) -> df$V3
df$V4 <- 1
df2 <- ddply(df,.(V3),transform,CSum=cumsum(V4))
df2 <- df2[ order(df2$V2), ]
df2$total <- cumsum(df2$V4)
df2$frac <- df2$CSum/df2$total
df2 <- rename(df2, c("V2"="Date", "V3"="Classe", "CSum"="Nombre total d'articles publiés"))
ggplot() + geom_line(aes(x=df2$Date,y=df2$"Nombre total d'articles publiés",color=df2$Classe)) + xlab("Date") + ylab("Nombre total d'articles publiés")

