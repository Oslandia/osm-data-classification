###############
# Introduction: data recovering
setwd("/home/rde/Documents/osland-ia")
tlnodes = read.csv("/home/rde/data/latest-bordeaux-metropole-node-timeline.csv",stringsAsFactors = F)
tlways = read.csv("/home/rde/data/latest-bordeaux-metropole-way-timeline.csv",stringsAsFactors = F)
tlrelations = read.csv("/home/rde/data/latest-bordeaux-metropole-relation-timeline.csv",stringsAsFactors = F)
save.image("/home/rde/Documents/osland-ia/data/bordeaux-metropole.RData")
#
View(tlnodes)
View(tlways)
View(tlrelations)
#
tlnodes$ts = as.POSIXct(tlnodes$ts)
tlways$ts = as.POSIXct(tlways$ts)
tlrelations$ts = as.POSIXct(tlrelations$ts)
###############
### Recover last version of each element
updatedelem = function(data){
  # Recover last version of nodes
  upddata = aggregate(version~id , data=data , FUN=max)
  upddata = merge(data,upddata)
  upddata = upddata[order(upddata$id),]
}
nodes = updatedelem(tlnodes)
ways = updatedelem(tlways)
relations = updatedelem(tlrelations)
###############

###############
### Introduction: building of the global metadata: data history with respect to user and change set ids
useractivity = cbind(elem=rep("n",nrow(tlnodes)),tlnodes[,c("id","version","uid","chgset","ts","ntags")])
useractivity = rbind( useractivity , cbind(elem=rep("w",nrow(tlways)),tlways[,c("id","version","uid","chgset","ts","ntags")]) )
useractivity = rbind( useractivity , cbind(elem=rep("r",nrow(tlrelations)),tlrelations[,c("id","version","uid","chgset","ts","ntags")]) )
useractivity = useractivity[order(useractivity$ts),]
str(useractivity)
# proportion of each element in the data history
table(useractivity$elem)

### Analyse 1: how many modifications per change set
modifbychgset = aggregate(cbind(elem,id)~chgset , data=useractivity, FUN=length)
modifbychgset = merge(modifbychgset[,-3] , unique(useractivity[,c("chgset","uid")]))[,c(1,3,2)]
head(modifbychgset)
plot(quantile(modifbychgset$elem,probs=seq(0,1,0.001)),seq(0,1,0.001),type="l",col="red",lwd=2,xlim=c(0,100),xlab="Modification quantity per change set",ylab="Empirical CDF")
abline(h=c(0,1),v=0)
nodemodifbychgset = setNames(aggregate(id~chgset , data=useractivity[useractivity$elem=="n",], FUN=length),c("chgset","nnodemodif"))
waymodifbychgset = setNames(aggregate(id~chgset , data=useractivity[useractivity$elem=="w",], FUN=length),c("chgset","nwaymodif"))
relationmodifbychgset = setNames(aggregate(id~chgset , data=useractivity[useractivity$elem=="r",], FUN=length),c("chgset","nrelationmodif"))
plot(quantile(nodemodifbychgset$nnodemodif,probs=seq(0,1,0.001)),seq(0,1,0.001),type="l",col="red",lwd=2,xlim=c(0,100),xlab="Modification per change set",ylab="Empirical CDF")
lines(quantile(waymodifbychgset$nwaymodif,probs=seq(0,1,0.001)),seq(0,1,0.001),col="blue",lwd=2)
lines(quantile(relationmodifbychgset$nrelationmodif,probs=seq(0,1,0.001)),seq(0,1,0.001),col="green",lwd=2)
abline(h=c(0,1),v=0)
legend("bottomright",legend=c("nodes","ways","relations"),col=c("red","blue","green"),lwd=2)
chgsetsynthesis = merge(modifbychgset,nodemodifbychgset,all=T)
chgsetsynthesis = merge(chgsetsynthesis,waymodifbychgset,all=T)
chgsetsynthesis = merge(chgsetsynthesis,relationmodifbychgset,all=T)
remove(nodemodifbychgset,waymodifbychgset,relationmodifbychgset)

# Analyse 2: how many change set per user
chgsetbyuser = aggregate(chgset~uid,data=modifbychgset,FUN=length)
head(chgsetbyuser)
quantile(chgsetbyuser$chgset,probs=seq(0,1,0.1))
plot(quantile(chgsetbyuser$chgset,probs=seq(0,1,0.001)),seq(0,1,0.001),type="l",col="red",xlim=c(0,100),xlab="Change set quantity per user",ylab="Empirical CDF")
abline(h=c(0,1),v=0)

# Analyse 3: how many modification per user
modifbyuser = aggregate(cbind(elem,id)~uid , data=useractivity, FUN=length)[,-3]
plot(quantile(modifbychgset$elem,probs=seq(0,1,0.001)),seq(0,1,0.001),type="l",col="red",lwd=2,xlim=c(0,100),xlab="Total modification per user",ylab="Empirical CDF")
abline(h=c(0,1),v=0)
head(modifbyuser)
usersynthesis = merge(chgsetbyuser,modifbyuser)
remove(chgsetbyuser,modifbyuser)
plot(usersynthesis$chgset,usersynthesis$elem,col="blue",pch=15,cex=0.5,xlab="Number of change set",ylab="Number of modified elements",main="User mapping")
abline(v=0,h=0)
plot(usersynthesis$chgset,usersynthesis$elem,col="blue",pch=15,cex=0.5,xlab="Number of change set",ylab="Number of modified elements",main="User mapping",xlim=c(0,1000),ylim=c(0,10000))
abline(v=0,h=0)
nodemodifperuser = setNames(aggregate(id~uid , data=useractivity[useractivity$elem=="n",], FUN=length),c("uid","nnodemodif"))
waymodifperuser = setNames(aggregate(id~uid , data=useractivity[useractivity$elem=="w",], FUN=length),c("uid","nwaymodif"))
relationmodifperuser = setNames(aggregate(id~uid , data=useractivity[useractivity$elem=="r",], FUN=length),c("uid","nrelationmodif"))
plot(quantile(nodemodifperuser$nnodemodif,probs=seq(0,1,0.001)),seq(0,1,0.001),type="l",col="red",lwd=2,xlim=c(0,200),xlab="Modification per user",ylab="Empirical CDF")
lines(quantile(waymodifperuser$nwaymodif,probs=seq(0,1,0.001)),seq(0,1,0.001),col="blue",lwd=2)
lines(quantile(relationmodifperuser$nrelationmodif,probs=seq(0,1,0.001)),seq(0,1,0.001),col="green",lwd=2)
abline(h=c(0,1),v=0)
legend("bottomright",legend=c("nodes","ways","relations"),col=c("red","blue","green"),lwd=2)
usersynthesis = merge(usersynthesis,nodemodifperuser,all=T)
usersynthesis = merge(usersynthesis,waymodifperuser,all=T)
usersynthesis = merge(usersynthesis,relationmodifperuser,all=T)

# Analyse 4: how many user per elements
userbyelem = aggregate(uid~elem+id,data=useractivity,FUN=function(x){length(unique(x))})
head(userbyelem)
plot(quantile(userbyelem[userbyelem$elem=="n","uid"],probs=seq(0,1,0.001)),seq(0,1,0.001),type="l",col="red",lwd=2,xlim=c(0,20),xlab="Unique contributor quantity per element",ylab="Empirical CDF")
lines(quantile(userbyelem[userbyelem$elem=="w","uid"],probs=seq(0,1,0.001)),seq(0,1,0.001),col="blue",lwd=2)
lines(quantile(userbyelem[userbyelem$elem=="r","uid"],probs=seq(0,1,0.001)),seq(0,1,0.001),col="green",lwd=2)
abline(h=c(0,1),v=0)
boxplot(userbyelem$uid~userbyelem$elem,ylim=c(0,10),col=c("pink","lightblue","lightgreen"),border=c("red","blue","green"))
par(mfrow=c(3,1))
hist(userbyelem[userbyelem$elem=="n","uid"],breaks=seq(0,130,1),xlim=c(0,10),ylim=c(0,1),freq=F,col="pink",border="red",main="",xlab="Number of unique contributors (nodes)")
hist(userbyelem[userbyelem$elem=="w","uid"],breaks=seq(0,130,1),xlim=c(0,10),ylim=c(0,1),freq=F,col="lightblue",border="blue",main="",xlab="Number of unique contributors (ways)")
hist(userbyelem[userbyelem$elem=="r","uid"],breaks=seq(0,130,1),xlim=c(0,10),ylim=c(0,1),freq=F,col="lightgreen",border="green",main="",xlab="Number of unique contributors (relations)")
par(mfrow=c(1,1))

# Analyse 5: how many version per elements
versionbyelem = aggregate(version~elem+id,data=useractivity,length)
head(versionbyelem)
elemsynthesis = merge(userbyelem,versionbyelem)
remove(userbyelem,versionbyelem)
plot(elemsynthesis$uid,elemsynthesis$version,col=elemsynthesis$elem,pch=3,cex=0.5,xlab="Number of unique contributors",ylab="Number of versions",main="Element building",ylim=c(0,600))
lines(1:2000,1:2000,lty="dotted",col="grey")
abline(v=0,h=0)
legend("topleft",legend=c("nodes","ways","relations"),col=1:3,pch=3)

# Analyse 6 : how many contributions per element and per user
contribbyuserelem = aggregate(version~elem+id+uid, data=useractivity, length)
head(contribbyuserelem)
plot(quantile(contribbyuserelem[contribbyuserelem$elem=="n","version"],probs=seq(0,1,0.001)),seq(0,1,0.001),type="l",col="red",lwd=2,xlim=c(0,10),xlab="Contribution per user per element",ylab="Empirical CDF")
lines(quantile(contribbyuserelem[contribbyuserelem$elem=="w","version"],probs=seq(0,1,0.001)),seq(0,1,0.001),col="blue",lwd=2)
lines(quantile(contribbyuserelem[contribbyuserelem$elem=="r","version"],probs=seq(0,1,0.001)),seq(0,1,0.001),col="green",lwd=2)
abline(h=c(0,1),v=0)
legend("bottomright",legend=c("nodes","ways","relations"),col=c("red","blue","green"),lwd=2)
nmodifperuserperelem = aggregate(version~uid, data=contribbyuserelem, median)
nnodemodifperuserperelem = setNames(aggregate(version~uid, data=contribbyuserelem[contribbyuserelem$elem=="n",], median),c("uid","nnodemodifperelem"))
nwaymodifperuserperelem = setNames(aggregate(version~uid, data=contribbyuserelem[contribbyuserelem$elem=="w",], median),c("uid","nwaymodifperelem"))
nrelationmodifperuserperelem = setNames(aggregate(version~uid, data=contribbyuserelem[contribbyuserelem$elem=="r",], median),c("uid","nrelationmodifperelem"))
usersynthesis = merge(usersynthesis,nmodifperuserperelem,all=T)
usersynthesis = merge(usersynthesis,nnodemodifperuserperelem,all=T)
usersynthesis = merge(usersynthesis,nwaymodifperuserperelem,all=T)
usersynthesis = merge(usersynthesis,nrelationmodifperuserperelem,all=T)
remove(nmodifperuserperelem,nnodemodifperuserperelem,nwaymodifperuserperelem,nrelationmodifperuserperelem)

# Analyse 7 : temporal description of user contributions
userfirst = setNames(aggregate(ts~uid , useractivity , min),c("uid","first"))
userlast = setNames(aggregate(ts~uid , useractivity , max),c("uid","last"))
usersynthesis = merge( usersynthesis , userfirst , all=T )
usersynthesis = merge( usersynthesis , userlast , all=T )
usersynthesis$activity.days = apply(usersynthesis , 1 , function(x){round(difftime(x[["last"]] , x[["first"]] , units="days")[[1]],2)})
plot(usersynthesis$nchgset , usersynthesis$activity.days , col="red",pch=3,cex=0.5,xlim=c(0,100),xlab="Change set per user",ylab="User activity (days)")
abline(v=0,h=0)

# Analyse 8 : temporal description of change sets
chgsetbegin = setNames(aggregate(ts~chgset , useractivity , min),c("chgset","begin"))
chgsetend = setNames(aggregate(ts~chgset , useractivity , max),c("chgset","end"))
chgsetsynthesis = merge( chgsetsynthesis , chgsetbegin , all=T )
chgsetsynthesis = merge( chgsetsynthesis , chgsetend , all=T )
chgsetsynthesis$duration = apply(chgsetsynthesis , 1 , function(x){round(difftime(x[["end"]] , x[["begin"]] , units="min")[[1]],2)})
plot(chgsetsynthesis$nnodemodif , chgsetsynthesis$duration , col="red",pch=3,cex=0.5,log="x",xlab="Modif per change set",ylab="Change set duration (min)")
points(chgsetsynthesis$nwaymodif , chgsetsynthesis$duration , col="blue",pch=3,cex=0.5)
points(chgsetsynthesis$nrelationmodif , chgsetsynthesis$duration , col="green",pch=3,cex=0.5,log="x")
abline(v=0,h=0)
legend("topright",legend=c("nodes","ways","relations"),col=c("red","blue","green"),pch=3)

# Analyse 9 : temporal description of element life cycle
elembegin = setNames(aggregate(ts~elem+id , useractivity , min),c("elem","id","creation"))
elemend = setNames(aggregate(ts~elem+id , useractivity , max),c("elem","id","lastmodif"))
elemsynthesis_ = elemsynthesis
elemsynthesis = merge( elemsynthesis , elembegin )
elemsynthesis = merge( elemsynthesis , elemend )
elemsynthesis$lifecycle = apply(elemsynthesis , 1 , function(x){round(difftime(x[["lastmodif"]] , x[["creation"]] , units="days")[[1]],2)})
elemsynthesis$available = rep(T,nrow(elemsynthesis))
delnodes = nodes[nodes$lon==Inf,"id"]
delways = ways[ways$nnodes==0,"id"]
delrelations = relations[relations$nmembers==0,"id"]
elemsynthesis[elemsynthesis$elem=="n"&elemsynthesis$id%in%delnodes,"available"] = F
elemsynthesis[elemsynthesis$elem=="w"&elemsynthesis$id%in%delways,"available"] = F
elemsynthesis[elemsynthesis$elem=="r"&elemsynthesis$id%in%delrelations,"available"] = F
timehorizon = as.POSIXct("2017-02-13 0:00:00")
elemsynt = elemsynthesis
elemsynthesis[elemsynthesis$available,"lifecycle"] = apply(elemsynthesis[elemsynthesis$available,] , 1 , function(x){round(difftime(timehorizon , x[["creation"]] , units="days")[[1]],2)})
elemsynthesis$lifeend = rep(timehorizon,nrow(elemsynthesis))
elemsynthesis[!elemsynthesis$available,"lifeend"] = elemsynthesis[!elemsynthesis$available,"lastmodif"]
remove(elemsynthesis_,elembegin,elemend)

### Summary: build a metadata structure for users, change sets and elements
# users
head(usersynthesis)
names(usersynthesis) = c("uid","nchgset","nmodif","nnodemodif","nwaymodif","nrelationmodif","nmodifperelem","nnodemodifperelem","nwaymodifperelem","nrelationmodifperelem","first","last","activity.days")
# change sets
head(chgsetsynthesis)
names(chgsetsynthesis) = c("chgset","uid","nmodif","nnodemodif","nwaymodif","nrelationmodif","begin","end","duration.min")
# elements
head(elemsynthesis)
names(elemsynthesis) = c("type","id","nuniqcontrib","nversion","creation","lastmodif","available","lifecycle.days","lifeend")

### Saving: keep important structures in .RData and .csv formats
save.image("/home/rde/Documents/osland-ia/data/bordeaux-metropole.RData")
#
write.csv(usersynthesis,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-users.csv")
write.csv(chgsetsynthesis,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-changesets.csv")
write.csv(elemsynthesis,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-elements.csv")
write.csv(useractivity,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-fullactivity.csv")
#
write.csv(nodes,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-nodes.csv")
write.csv(ways,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-ways.csv")
write.csv(relations,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-relations.csv")
#
write.csv(tlnodes,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-node-history.csv")
write.csv(tlways,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-way-history.csv")
write.csv(tlrelations,"/home/rde/Documents/osland-ia/data/bordeaux-metropole-relation-history.csv")
