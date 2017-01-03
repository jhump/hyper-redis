java_library(
    name='base',
    sources=globs("*.java", "channel/*.java", "protocol/*.java"),
    dependencies=['3rdparty:guava', '3rdparty:jsr305', '3rdparty:netty'],
    )

java_library(
    name='redis',
    dependencies=[':base']
    )
