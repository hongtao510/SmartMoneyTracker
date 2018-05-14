library(ggplot2)
library(ggthemes)

df = data.frame(vol = rnorm(500, 100, 3), x_label = 1:500)

df$vol[20]=300
df$vol[200]=300
df$vol[250]=700



ggplot(data=df, aes(x=x_label, y=vol)) +
	geom_bar(stat="identity", fill="steelblue")+
	scale_y_continuous(limits = c(0, 720))+
  	scale_x_continuous(limits = c(1, 300), breaks = c(1, 100, 200, 300), labels=c('09:30 AM EST', '09:35 AM EST', '09:40 AM EST', '09:50 AM EST'))+
	xlab("Time") +
  	ylab("$SPX Option Volume") +
  	geom_hline(aes(yintercept=100), color="red", size=1.8)+
	theme_economist()+
	theme(text = element_text(size=16), 
		  axis.title.x = element_text(size=16, face="bold"),
		  axis.title.y = element_text(size=16, face="bold")
		  ) 


