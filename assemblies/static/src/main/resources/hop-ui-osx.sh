
JAVA_OPTIONS='-Xmx1g'

# A special option for OSX
#
JAVA_OPTIONS="-XstartOnFirstThread ${JAVA_OPTIONS}"

# Uncomment to allow remote debugging
#
# JAVA_OPTIONS="${JAVA_OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"

java ${JAVA_OPTIONS} -classpath 'lib/*:libswt/osx64/*' org.apache.hop.ui.hopui.HopUi

