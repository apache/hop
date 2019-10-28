
JAVA_OPTIONS='-Xmx1g'

# Uncomment to allow remote debugging
#
# JAVA_OPTIONS="${JAVA_OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"

java ${JAVA_OPTIONS} -classpath 'lib/*:libswt/linux/x86_64/*' org.apache.hop.ui.hopui.HopUi

