grammar HyperRedis;

// The file that describes redis commands consists of multiple blocks.
file : block+;

// Each block has a name (preceded by optional doc comments) and one or more commands.
block : DocComment* Identifier Bang? OpenBrace
        command+
        CloseBrace;

// Commands can be preceded by optional doc comments and
// describe arguments and optional return types.        
command : DocComment* commandName arg*
          ( arrow stream? type (Colon qualifiedIdentifier)? )?
          version? rename? Semicolon;

commandName : Question? name;

name : Identifier ( Equals Identifier )?;

arg : param | syntheticParam | symbol | optional | choice | repeatedChoice;

param : Minus? paramDef ( paramExpansion ArrayIndicator? )?;

paramDef : ( Identifier Equals )? type;

paramExpansion : Colon Colon OpenBrace synthetic ( synthetic )* CloseBrace;

synthetic : type Colon Embedded;

syntheticParam : Plus synthetic;

symbol : Colon Question? name;

optional : OpenBracket argList CloseBracket;

choice : OpenBrace argList ( Comma argList )+ CloseBrace;

argList : arg+;

repeatedChoice : Asterisk OpenBrace symbolArg ( Comma symbolArg )* CloseBrace;

symbolArg : symbol arg*;

arrow : Minus CloseAngle;

stream : 'stream';

type : declaredType ( ArrayIndicator )*;

declaredType : qualifiedIdentifier typeArgs?;

qualifiedIdentifier : Identifier ( Period Identifier )*;

typeArgs : OpenAngle typeArg ( ',' typeArg )* CloseAngle;

typeArg : type | wildcardType;

wildcardType : Question ( wildcardBound )?;

wildcardBound : 'extends' type | 'super' type;

version : At VersionNumber;

rename : wideArrow Identifier version?;

wideArrow : Equals Equals CloseAngle;

// Terminals

DocComment : '//' ~[\r\n]*;
LineComment : '#' ~[\r\n]* -> skip;
Embedded : '`' ~[`]* '`';
Ellipsis : '...';
Whitespace : [ \t\r\n\u000C]+ -> skip;
ArrayIndicator : OpenBracket CloseBracket;

Identifier : Alpha Alphanumeric*;
fragment Alpha : [a-zA-Z_];
fragment Alphanumeric : [a-zA-Z0-9_\-];

VersionNumber : Numeric+ ( Period Numeric+ )*;
fragment Numeric : [0-9];

Period : '.';
Colon : ':';
Semicolon : ';';
Bang : '!';
Question : '?';
Asterisk : '*';
At : '@';
Minus : '-';
Plus : '+';
OpenBracket : '[';
CloseBracket : ']';
OpenBrace : '{';
CloseBrace : '}';
OpenAngle : '<';
CloseAngle : '>';
Comma : ',';
Equals : '=';
Pipe : '|';
