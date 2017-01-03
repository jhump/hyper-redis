java_antlr_library(
    name='antlr',
    sources=['HyperRedis.g4'],
    compiler='antlr4',
    package='com.bluegosling.redis.generator'
    )

java_library(
    name='generator',
    sources=rglobs("*.java"),
    dependencies=['src/com/bluegosling/redis:base',
                ':antlr','3rdparty:antlr-runtime',
                '3rdparty:javapoet', '3rdparty:guava',
                '3rdparty:jsr305', '3rdparty:moshi'],
    )
