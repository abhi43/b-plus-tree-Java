JAR=btreelib.jar

#this is the name of the given project folder
ASSIGNMENT=btree_project_fall_21

#change the ASSIGN path to the path where you have downloaded
ASSIGN=/Users/sanky/Workspace

#change the JDKPATH if you are not using omega.uta.edu
JDKPATH = /usr
LIBPATH = $(ASSIGN)/$(ASSIGNMENT)/lib/$(JAR)
CLASSPATH = $(LIBPATH):$(ASSIGN)/$(ASSIGNMENT)/src
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

BTTest:BTTest.java
	$(JAVAC) BTTest.java TestDriver.java

bttest: BTTest
	$(JAVA) tests.BTTest

clean:
	\rm -f *.class *~ \#* core
