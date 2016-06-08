/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/

/*
  This is an interface to the Attribute-Value-Units type of metadata.
*/

#include "rods.h"
#include "rodsClient.h"
#include "irods_client_api_table.hpp"
#include "irods_pack_table.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "query2.hpp"

#define MAX_SQL 300
#define BIG_STR 3000

char cwd[BIG_STR];

int debug = 0;
int testMode = 0; /* some particular internal tests */
int longMode = 0; /* more detailed listing */
int upperCaseFlag = 0;

char zoneArgument[MAX_NAME_LEN + 2] = "";

rcComm_t *Conn;
rodsEnv myEnv;

int lastCommandStatus = 0;
int printCount = 0;

int usage( char *subOpt );

/*
 print the results of a general query.
 */
void
printGenQueryResults( rcComm_t *Conn, int status, char *json,
                      char *descriptions[] ) {

  std::vector< std::vector<std::string> > results = parseQuery2Results(json);

  char localTime[TIME_LEN];
  lastCommandStatus = status;
  if ( status == CAT_NO_ROWS_FOUND ) {
    lastCommandStatus = 0;
  }
  if ( status != 0 && status != CAT_NO_ROWS_FOUND ) {
    printError( Conn, status, "rcGenQuery" );
  }
  else {
    if ( status == CAT_NO_ROWS_FOUND ) {
      if ( printCount == 0 ) {
	printf( "None\n" );
      }
    }
    else {
      for ( size_t i = 0; i < results.size(); i++ ) {
	if ( i > 0 ) {
	  printf( "----\n" );
	}
	auto row = results[i];
	for ( size_t j = 0; j < row.size(); j++ ) {
	  std::string tResult;
	  tResult = row[j];
	  if ( *descriptions[j] != '\0' ) {
	    if ( strstr( descriptions[j], "time" ) != 0 ) {
	      getLocalTimeFromRodsTime( tResult.c_str(), localTime );
	      printf( "%s: %s\n", descriptions[j],
		      localTime );
	    }
	    else {
	      printf( "%s: %s\n", descriptions[j], tResult.c_str() );
	      printCount++;
	    }
	  }
	}
      }
    }
  }
}

/*
 Via a general query and show the AVUs for a dataobject.
 */
int
showDataObj( char *name, char *attrName, int wild ) {
    char fullName[MAX_NAME_LEN];
    char myDirName[MAX_NAME_LEN];
    char myFileName[MAX_NAME_LEN];
    int status;
    /* "id" only used in testMode, in longMode id is reset to be 'time set' :*/
    char *columnNames[] = {"attribute", "value", "units", "id"};

    std::string qu;
    char *res;

    printf( "AVUs defined for dataObj %s:\n", name );
    printCount = 0;


    if ( *name == '/' ) {
        snprintf( fullName, sizeof( fullName ), "%s", name );
    }
    else {
        snprintf( fullName, sizeof( fullName ), "%s/%s", cwd, name );
    }

    if ( int status = splitPathByKey( fullName, myDirName,
				      MAX_NAME_LEN, myFileName, MAX_NAME_LEN, '/' ) ) {
      rodsLog( LOG_ERROR, "splitPathByKey failed in showDataObj with status %d", status );
    }

    qu = "COLL_NAME(cid,\"";
    qu += myDirName;
    qu += "\")";
    
    qu = "DATA_NAME_2(oid,\"";
    qu += myFileName;
    qu += "\") DATA_COLL_ID(oid, cid) META_2(oid, a, v, u)";

    if ( attrName != NULL && *attrName != '\0' ) {
        if ( wild ) {
            qu  = "like(a,\"";
            qu += attrName;
            qu += "\"";
        }
        else {
            qu  = "eq(a,\"";
            qu += attrName;
            qu += "\"";
        }
    }

    status = query(Conn, qu.c_str(), "a v u", &res);

    printGenQueryResults( Conn, status, res, columnNames );
    
    return 0;
}

/*
Via a general query, show the AVUs for a collection
*/
int
showColl( char *name, char *attrName, int wild ) {
    char fullName[MAX_NAME_LEN];
    int status;
    /* "id" only used in testMode, in longMode id is reset to be 'time set' :*/
    char *columnNames[] = {"attribute", "value", "units", "id"};

    std::string qu;
    char *res;

    printf( "AVUs defined for collection %s:\n", name );
    printCount = 0;


    if ( *name == '/' ) {
        snprintf( fullName, sizeof( fullName ), "%s", name );
    }
    else {
        snprintf( fullName, sizeof( fullName ), "%s/%s", cwd, name );
    }

    qu = "COLL_NAME(cid,\"";
    qu += fullName;
    qu += "\") META2(cid, a, v, u)";

    if ( attrName != NULL && *attrName != '\0' ) {
        if ( wild ) {
            qu  = "like(a,\"";
            qu += attrName;
            qu += "\"";
        }
        else {
            qu  = "eq(a,\"";
            qu += attrName;
            qu += "\"";
        }
    }
    

    status = query(Conn, qu.c_str(), "a v u", &res);

    printGenQueryResults( Conn, status, res, columnNames );
    
    return 0;
}

/*
Via a general query, show the AVUs for a resource
*/
int
showResc( char *name, char *attrName, int wild ) {
    int status;
    /* "id" only used in testMode, in longMode id is reset to be 'time set' :*/
    char *columnNames[] = {"attribute", "value", "units", "id"};

    std::string qu;
    char *res;

    printf( "AVUs defined for resource %s:\n", name );
    printCount = 0;


    qu = "RESC_NAME(rid,\"";
    qu += name;
    qu += "\") META2(rid, a, v, u)";

    if ( attrName != NULL && *attrName != '\0' ) {
        if ( wild ) {
            qu  = "like(a,\"";
            qu += attrName;
            qu += "\"";
        }
        else {
            qu  = "eq(a,\"";
            qu += attrName;
            qu += "\"";
        }
    }
    

    status = query(Conn, qu.c_str(), "a v u", &res);

    printGenQueryResults( Conn, status, res, columnNames );
    
    return 0;
}

/*
Via a general query, show the AVUs for a user
*/
int
showUser( char *name, char *attrName, int wild ) {
    int status;
    /* "id" only used in testMode, in longMode id is reset to be 'time set' :*/
    char *columnNames[] = {"attribute", "value", "units", "id"};

    std::string qu;
    char *res;

    printf( "AVUs defined for collection %s:\n", name );
    printCount = 0;


    qu = "USER_NAME(uid,\"";
    qu += name;
    qu += "\") META2(uid, a, v, u)";

    if ( attrName != NULL && *attrName != '\0' ) {
        if ( wild ) {
            qu  = "like(a,\"";
            qu += attrName;
            qu += "\"";
        }
        else {
            qu  = "eq(a,\"";
            qu += attrName;
            qu += "\"";
        }
    }
    

    status = query(Conn, qu.c_str(), "a v u", &res);

    printGenQueryResults( Conn, status, res, columnNames );
    
    return 0;
}

/*
 Modify (add or remove) AVUs
 */
int
modAVUMetadata( char *arg0, char *arg1, char *arg2, char *arg3,
                char *arg4, char *arg5, char *arg6, char *arg7, char *arg8 ) {
    int status;
    char fullName[MAX_NAME_LEN];
    char myFileName[MAX_NAME_LEN];
    char myDirName[MAX_NAME_LEN];

    std::string qu;
    if ( strcmp( arg1, "-R" ) == 0 || strcmp( arg1, "-r" ) == 0 ||
            strcmp( arg1, "-u" ) == 0 ) {
        snprintf( fullName, sizeof( fullName ), "%s", arg2 );
    }
    else if ( strlen( arg2 ) > 0 ) {
        if ( *arg2 == '/' ) {
            snprintf( fullName, sizeof( fullName ), "%s", arg2 );
        }
        else {
            snprintf( fullName, sizeof( fullName ), "%s/%s", cwd, arg2 );
        }
    }
    else {
        snprintf( fullName, sizeof( fullName ), "%s", cwd );
    }

    if(strcmp(arg0, "add") == 0) {
      if ( strcmp( arg1, "-R" ) == 0 || strcmp( arg1, "-r" ) == 0) {
        snprintf( fullName, sizeof( fullName ), "%s", arg2 );
	qu += "RESC_NAME(id, \"" ;
	qu += fullName ;
	qu += "\") insert META_2(id, \"";
	qu +=  arg3 ;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
      } else if (strcmp( arg1, "-u" ) == 0 ) {
        snprintf( fullName, sizeof( fullName ), "%s", arg2 );
	qu += "USER_NAME(id, \"";
	qu += fullName;
	qu += "\") insert META_2(id, \"";
	qu += arg3;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
      }
      else if ( strcmp( arg1, "-C" ) == 0 ) {
	qu += "COLL_NAME(id, \"" ;
	qu += fullName;
	qu += "\") insert META_2(id, \"";
	qu += arg3;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
      }
      else {
	if ( int status = splitPathByKey( fullName, myDirName,
					  MAX_NAME_LEN, myFileName, MAX_NAME_LEN, '/' ) ) {
	  rodsLog( LOG_ERROR, "splitPathByKey failed in showDataObj with status %d", status );
	}
	qu += "DATA_NAME_2(oid, \"";
	qu += myFileName;
	qu += "\") COLL_NAME(cid, \"";
	qu += myDirName;
	qu += "\") DATA_COLL_ID(oid, cid) insert META_2(oid, \"";
	qu += arg3;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
      }
    } else {
      if ( strcmp( arg1, "-R" ) == 0 || strcmp( arg1, "-r" ) == 0) {
        snprintf( fullName, sizeof( fullName ), "%s", arg2 );
	qu += "RESC_NAME(id, \"";
	qu += fullName;
	qu += "\") delete META_2(id, \"";
	qu += arg3;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
	
      } else if (
		 strcmp( arg1, "-u" ) == 0 ) {
        snprintf( fullName, sizeof( fullName ), "%s", arg2 );
	qu += "USER_NAME(id, \"";
	qu += fullName;
	qu += "\") delete META_2(id, \"";
	qu += arg3;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
      }
      else if ( strcmp( arg1, "-C" ) == 0 ) {
	qu += "COLL_NAME(id, \"";
	qu += fullName;
	qu += "\") delete META_2(id, \"";
	qu += arg3;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
      }
      else {
	if ( int status = splitPathByKey( fullName, myDirName,
					  MAX_NAME_LEN, myFileName, MAX_NAME_LEN, '/' ) ) {
	  rodsLog( LOG_ERROR, "splitPathByKey failed in showDataObj with status %d", status );
	}
	qu += "DATA_NAME_2(oid, \"";
	qu += myFileName;
	qu += "\") COLL_NAME(cid, \"";
	qu += myDirName;
	qu += "\") DATA_COLL_ID(oid, cid) delete META_2(oid, \"";
	qu += arg3;
	qu += "\",\"";
	qu += arg4;
	qu += "\", \"";
	qu += arg5;
	qu += "\")";
      }
    }


    char *res;
    status = query(Conn, qu.c_str(), "", &res);
    lastCommandStatus = status;

    if ( status < 0 ) {
        if ( Conn->rError ) {
            rError_t *Err;
            rErrMsg_t *ErrMsg;
            int i, len;
            Err = Conn->rError;
            len = Err->len;
            for ( i = 0; i < len; i++ ) {
                ErrMsg = Err->errMsg[i];
                rodsLog( LOG_ERROR, "Level %d: %s", i, ErrMsg->msg );
            }
        }
        char *mySubName = NULL;
        const char *myName = rodsErrorName( status, &mySubName );
        rodsLog( LOG_ERROR, "rcModAVUMetadata failed with error %d %s %s",
                 status, myName, mySubName );
        free( mySubName );
    }
    return status;
}

/*
 Prompt for input and parse into tokens
*/
int
getInput( char *cmdToken[], int maxTokens ) {
    int lenstr, i;
    static char ttybuf[BIG_STR];
    int nTokens;
    int tokenFlag; /* 1: start reg, 2: start ", 3: start ' */
    char *cpTokenStart;
    char *stat;

    memset( ttybuf, 0, BIG_STR );
    fputs( "imeta>", stdout );
    stat = fgets( ttybuf, BIG_STR, stdin );
    if ( stat == 0 ) {
        printf( "\n" );
        rcDisconnect( Conn );
        if ( lastCommandStatus != 0 ) {
            exit( 4 );
        }
        exit( 0 );
    }
    lenstr = strlen( ttybuf );
    for ( i = 0; i < maxTokens; i++ ) {
        cmdToken[i] = "";
    }
    cpTokenStart = ttybuf;
    nTokens = 0;
    tokenFlag = 0;
    for ( i = 0; i < lenstr; i++ ) {
        if ( ttybuf[i] == '\n' ) {
            ttybuf[i] = '\0';
            cmdToken[nTokens++] = cpTokenStart;
            return 0;
        }
        if ( tokenFlag == 0 ) {
            if ( ttybuf[i] == '\'' ) {
                tokenFlag = 3;
                cpTokenStart++;
            }
            else if ( ttybuf[i] == '"' ) {
                tokenFlag = 2;
                cpTokenStart++;
            }
            else if ( ttybuf[i] == ' ' ) {
                cpTokenStart++;
            }
            else {
                tokenFlag = 1;
            }
        }
        else if ( tokenFlag == 1 ) {
            if ( ttybuf[i] == ' ' ) {
                ttybuf[i] = '\0';
                cmdToken[nTokens++] = cpTokenStart;
                cpTokenStart = &ttybuf[i + 1];
                tokenFlag = 0;
            }
        }
        else if ( tokenFlag == 2 ) {
            if ( ttybuf[i] == '"' ) {
                ttybuf[i] = '\0';
                cmdToken[nTokens++] = cpTokenStart;
                cpTokenStart = &ttybuf[i + 1];
                tokenFlag = 0;
            }
        }
        else if ( tokenFlag == 3 ) {
            if ( ttybuf[i] == '\'' ) {
                ttybuf[i] = '\0';
                cmdToken[nTokens++] = cpTokenStart;
                cpTokenStart = &ttybuf[i + 1];
                tokenFlag = 0;
            }
        }
        if ( nTokens >= maxTokens ) {
            printf( "Limit reached (too many tokens, unrecognized input\n" );
            return -1;
        }
    }
    return 0;
}

/*
 Detect a 'l' in a '-' option and if present, set a mode flag and
 remove it from the string (to simplify other processing).
 */
void
handleMinusL( char *token ) {
    char *cptr, *cptr2;
    if ( *token != '-' ) {
        return;
    }
    for ( cptr = token++; *cptr != '\0'; cptr++ ) {
        if ( *cptr == 'l' ) {
            longMode = 1;
            for ( cptr2 = cptr; *cptr2 != '\0'; cptr2++ ) {
                *cptr2 = *( cptr2 + 1 );
            }
            return;
        }
    }
    longMode = 0;
}

/* handle a command,
   return code is 0 if the command was (at least partially) valid,
   -1 for quitting,
   -2 for if invalid
   -3 if empty.
 */
int
doCommand( char *cmdToken[] ) {
    int wild, doLs;
    if ( strcmp( cmdToken[0], "help" ) == 0 ||
            strcmp( cmdToken[0], "h" ) == 0 ) {
        usage( cmdToken[1] );
        return 0;
    }
    if ( strcmp( cmdToken[0], "quit" ) == 0 ||
            strcmp( cmdToken[0], "q" ) == 0 ) {
        return -1;
    }
    if ( strcmp( cmdToken[0], "-v" ) == 0 ||
            strcmp( cmdToken[0], "-V" ) == 0 ) {
        return -3;
    }

    handleMinusL( cmdToken[1] );
    handleMinusL( cmdToken[2] );

    if ( strcmp( cmdToken[1], "-C" ) == 0 ) {
        cmdToken[1][1] = 'c';
    }
    if ( strcmp( cmdToken[1], "-D" ) == 0 ) {
        cmdToken[1][1] = 'd';
    }
    if ( strcmp( cmdToken[1], "-R" ) == 0 ) {
        cmdToken[1][1] = 'r';
    }
    if ( strcmp( cmdToken[2], "-C" ) == 0 ) {
        cmdToken[2][1] = 'c';
    }
    if ( strcmp( cmdToken[2], "-D" ) == 0 ) {
        cmdToken[2][1] = 'd';
    }
    if ( strcmp( cmdToken[2], "-R" ) == 0 ) {
        cmdToken[2][1] = 'r';
    }

    if ( strcmp( cmdToken[0], "adda" ) == 0 ) {
        modAVUMetadata( "adda", cmdToken[1], cmdToken[2],
                        cmdToken[3], cmdToken[4], cmdToken[5],
                        cmdToken[6], cmdToken[7], "" );
        return 0;
    }
    if ( strcmp( cmdToken[0], "add" ) == 0 ) {
        modAVUMetadata( "add", cmdToken[1], cmdToken[2],
                        cmdToken[3], cmdToken[4], cmdToken[5],
                        cmdToken[6], cmdToken[7], "" );
        return 0;
    }
    if ( strcmp( cmdToken[0], "addw" ) == 0 ) {
        int myStat;
        myStat = modAVUMetadata( "addw", cmdToken[1], cmdToken[2],
                                 cmdToken[3], cmdToken[4], cmdToken[5],
                                 cmdToken[6], cmdToken[7], "" );
        if ( myStat > 0 ) {
            printf( "AVU added to %d data-objects\n", myStat );
            lastCommandStatus = 0;
        }
        return 0;
    }
    if ( strcmp( cmdToken[0], "rmw" ) == 0 ) {
        modAVUMetadata( "rmw", cmdToken[1], cmdToken[2],
                        cmdToken[3], cmdToken[4], cmdToken[5],
                        cmdToken[6], cmdToken[7], "" );
        return 0;
    }
    if ( strcmp( cmdToken[0], "rm" ) == 0 ) {
        modAVUMetadata( "rm", cmdToken[1], cmdToken[2],
                        cmdToken[3], cmdToken[4], cmdToken[5],
                        cmdToken[6], cmdToken[7], "" );
        return 0;
    }
    if ( testMode ) {
        if ( strcmp( cmdToken[0], "rmi" ) == 0 ) {
            modAVUMetadata( "rmi", cmdToken[1], cmdToken[2],
                            cmdToken[3], cmdToken[4], cmdToken[5],
                            cmdToken[6], cmdToken[7], "" );
            return 0;
        }
    }
    if ( strcmp( cmdToken[0], "mod" ) == 0 ) {
        modAVUMetadata( "mod", cmdToken[1], cmdToken[2],
                        cmdToken[3], cmdToken[4], cmdToken[5],
                        cmdToken[6], cmdToken[7], cmdToken[8] );
        return 0;
    }
    if ( strcmp( cmdToken[0], "set" ) == 0 ) {
        modAVUMetadata( "set", cmdToken[1], cmdToken[2],
                        cmdToken[3], cmdToken[4], cmdToken[5],
                        cmdToken[6], cmdToken[7], "" );
        return 0;
    }

    doLs = 0;
    if ( strcmp( cmdToken[0], "lsw" ) == 0 ) {
        doLs = 1;
        wild = 1;
    }
    if ( strcmp( cmdToken[0], "ls" ) == 0 ) {
        doLs = 1;
        wild = 0;
    }
    if ( doLs ) {
        if ( strcmp( cmdToken[1], "-d" ) == 0 ) {
            showDataObj( cmdToken[2], cmdToken[3], wild );
            return 0;
        }
        if ( strcmp( cmdToken[1], "-C" ) == 0 || strcmp( cmdToken[1], "-c" ) == 0 ) {
            showColl( cmdToken[2], cmdToken[3], wild );
            return 0;
        }
        if ( strcmp( cmdToken[1], "-R" ) == 0 || strcmp( cmdToken[1], "-r" ) == 0 ) {
            showResc( cmdToken[2], cmdToken[3], wild );
            return 0;
        }
        if ( strcmp( cmdToken[1], "-u" ) == 0 ) {
            showUser( cmdToken[2], cmdToken[3], wild );
            return 0;
        }
    }


    if ( *cmdToken[0] != '\0' ) {
        printf( "unrecognized subcommand '%s', try 'imeta help'\n", cmdToken[0] );
        return -2;
    }
    return -3;
}

int
main( int argc, char **argv ) {

    signal( SIGPIPE, SIG_IGN );

    int status, i, j;
    rErrMsg_t errMsg;

    rodsArguments_t myRodsArgs;


    int argOffset;

    int maxCmdTokens = 40;
    char *cmdToken[40];
    int keepGoing;
    int firstTime;

    rodsLogLevel( LOG_ERROR );

    status = parseCmdLineOpt( argc, argv, "vVhgrcGRCdulz:", 0, &myRodsArgs );
    if ( status ) {
        printf( "Use -h for help.\n" );
        exit( 1 );
    }
    if ( myRodsArgs.help == True ) {
        usage( "" );
        exit( 0 );
    }

    if ( myRodsArgs.zone == True ) {
        strncpy( zoneArgument, myRodsArgs.zoneName, MAX_NAME_LEN );
    }

    if ( myRodsArgs.longOption ) {
        longMode = 1;
    }

    argOffset = myRodsArgs.optind;
    if ( argOffset > 1 ) {
        if ( argOffset > 2 ) {
            if ( *argv[1] == '-' && *( argv[1] + 1 ) == 'z' ) {
                if ( *( argv[1] + 2 ) == '\0' ) {
                    argOffset = 3; /* skip -z zone */
                }
                else {
                    argOffset = 2; /* skip -zzone */
                }
            }
            else {
                argOffset = 1; /* Ignore the parseCmdLineOpt parsing
                as -d etc handled  below*/
            }
        }
        else {
            argOffset = 1; /* Ignore the parseCmdLineOpt parsing
             as -d etc handled  below*/
        }
    }

    status = getRodsEnv( &myEnv );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "main: getRodsEnv error. status = %d",
                 status );
        exit( 1 );
    }
    strncpy( cwd, myEnv.rodsCwd, BIG_STR );
    if ( strlen( cwd ) == 0 ) {
        strcpy( cwd, "/" );
    }

    for ( i = 0; i < maxCmdTokens; i++ ) {
        cmdToken[i] = "";
    }
    j = 0;
    for ( i = argOffset; i < argc; i++ ) {
        if ( j >= maxCmdTokens ) {
            printf( "Unrecognized input, too many input tokens\n" );
            exit( 4 );
        }
        cmdToken[j++] = argv[i];
    }

#if defined(linux_platform)
    /*
      imeta cp -d TestFile1 -d TestFile3
      comes in as:     -d -d cp TestFile1 TestFile3
      so switch it to: cp -d -d TestFile1 TestFile3
    */
    if ( cmdToken[0] != NULL && *cmdToken[0] == '-' ) {
        /* args were toggled, switch them back */
        if ( cmdToken[1] != NULL && *cmdToken[1] == '-' ) {
            if ( argv[argOffset + 2] ) { cmdToken[0] = argv[argOffset + 2]; }
            if ( argv[argOffset] ) { cmdToken[1] = argv[argOffset]; }
            if ( argv[argOffset + 1] ) { cmdToken[2] = argv[argOffset + 1]; }
        }
        else {
            if ( argv[argOffset + 1] ) { cmdToken[0] = argv[argOffset + 1]; }
            if ( argv[argOffset] ) { cmdToken[1] = argv[argOffset]; }
        }
    }
#else
    /* tested on Solaris, not sure other than Linux/Solaris */
    /*
      imeta cp -d TestFile1 -d TestFile3
      comes in as:     cp -d TestFile1 -d TestFile3
      so switch it to: cp -d -d TestFile1 TestFile3
    */
    if ( cmdToken[0] != NULL && cmdToken[1] != NULL && *cmdToken[1] == '-' &&
            cmdToken[2] != NULL && cmdToken[3] != NULL && *cmdToken[3] == '-' ) {
        /* two args */
        cmdToken[2] = argv[argOffset + 3];
        cmdToken[3] = argv[argOffset + 2];
    }

#endif

    if ( strcmp( cmdToken[0], "help" ) == 0 ||
            strcmp( cmdToken[0], "h" ) == 0 ) {
        usage( cmdToken[1] );
        exit( 0 );
    }

    if ( strcmp( cmdToken[0], "spass" ) == 0 ) {
        char scrambled[MAX_PASSWORD_LEN + 100];
        if ( strlen( cmdToken[1] ) > MAX_PASSWORD_LEN - 2 ) {
            printf( "Password exceeds maximum length\n" );
        }
        else {
            obfEncodeByKey( cmdToken[1], cmdToken[2], scrambled );
            printf( "Scrambled form is:%s\n", scrambled );
        }
        exit( 0 );
    }

    // =-=-=-=-=-=-=-
    // initialize pluggable api table
    irods::api_entry_table&  api_tbl = irods::get_client_api_table();
    irods::pack_entry_table& pk_tbl  = irods::get_pack_table();
    init_api_table( api_tbl, pk_tbl );

    Conn = rcConnect( myEnv.rodsHost, myEnv.rodsPort, myEnv.rodsUserName,
                      myEnv.rodsZone, 0, &errMsg );

    if ( Conn == NULL ) {
        char *mySubName = NULL;
        const char *myName = rodsErrorName( errMsg.status, &mySubName );
        rodsLog( LOG_ERROR, "rcConnect failure %s (%s) (%d) %s",
                 myName,
                 mySubName,
                 errMsg.status,
                 errMsg.msg );
        free( mySubName );

        exit( 2 );
    }

    status = clientLogin( Conn );
    if ( status != 0 ) {
        if ( !debug ) {
            exit( 3 );
        }
    }

    keepGoing = 1;
    firstTime = 1;
    while ( keepGoing ) {
        int status;
        status = doCommand( cmdToken );
        if ( status == -1 ) {
            keepGoing = 0;
        }
        if ( firstTime ) {
            if ( status == 0 ) {
                keepGoing = 0;
            }
            if ( status == -2 ) {
                keepGoing = 0;
                lastCommandStatus = -1;
            }
            firstTime = 0;
        }
        if ( keepGoing ) {
            status = getInput( cmdToken, maxCmdTokens );
            if ( status < 0 ) {
                lastCommandStatus = status;
                break;
            }
        }
    }

    printErrorStack( Conn->rError );

    rcDisconnect( Conn );

    if ( lastCommandStatus != 0 ) {
        exit( 4 );
    }
    exit( 0 );
}

/*
Print the main usage/help information.
 */
void usageMain() {
    char *msgs[] = {
        "Usage: imeta [-vVhz] [command]",
        " -v verbose",
        " -V Very verbose",
        " -z Zonename  work with the specified Zone",
        " -h This help",
        "Commands are:",
        " add -d|C|R|u Name AttName AttValue [AttUnits] (Add new AVU triple)",
        " adda -d|C|R|u Name AttName AttValue [AttUnits] (Add as administrator)",
        "                                     (same as 'add' but bypasses ACLs)",
        " addw -d Name AttName AttValue [AttUnits] (Add new AVU triple",
        "                                           using Wildcards in Name)",
        " rm  -d|C|R|u Name AttName AttValue [AttUnits] (Remove AVU)",
        " rmw -d|C|R|u Name AttName AttValue [AttUnits] (Remove AVU, use Wildcards)",
        " rmi -d|C|R|u Name MetadataID (Remove AVU by MetadataID)",
        " mod -d|C|R|u Name AttName AttValue [AttUnits] [n:Name] [v:Value] [u:Units]",
        "      (modify AVU; new name (n:), value(v:), and/or units(u:)",
        " set -d|C|R|u Name AttName newValue [newUnits] (Assign a single value)",
        " ls  -[l]d|C|R|u Name [AttName] (List existing AVUs for item Name)",
        " lsw -[l]d|C|R|u Name [AttName] (List existing AVUs, use Wildcards)",
        " qu -d|C|R|u AttName Op AttVal [...] (Query objects with matching AVUs)",
        " cp -d|C|R|u -d|C|R|u Name1 Name2 (Copy AVUs from item Name1 to Name2)",
        " upper (Toggle between upper case mode for queries (qu))",
        " ",
        "Metadata attribute-value-units triples (AVUs) consist of an Attribute-Name,",
        "Attribute-Value, and an optional Attribute-Units.  They can be added",
        "via the 'add' command (and in other ways), and",
        "then queried to find matching objects.",
        " ",
        "For each command, -d, -C, -R, or -u is used to specify which type of",
        "object to work with: dataobjs (iRODS files), collections, resources,",
        "or users.",
        " ",
        "Fields represented with upper case, such as Name, are entered values.  For",
        "example, 'Name' is the name of a dataobject, collection, resource,",
        "or user.",
        " ",
        "For rmw and lsw, the % and _ wildcard characters (as defined for SQL) can",
        "be used for matching attribute values.",
        " ",
        "For addw, the % and _ wildcard characters (as defined for SQL) can",
        "be used for matching object names.  This is currently implemented only",
        "for data-objects (-d).",
        " ",
        "A blank execute line invokes the interactive mode, where imeta",
        "prompts and executes commands until 'quit' or 'q' is entered.",
        "Like other unix utilities, a series of commands can be piped into it:",
        "'cat file1 | imeta' (maintaining one connection for all commands).",
        " ",
        "Single or double quotes can be used to enter items with blanks.",
        " ",
        "Entered usernames are of the form username[#zone].  If #zone is not",
        "provided, the zone from your irods_environment.json is assumed.",
        " ",
        "The appropriate zone (local or remote) is determined from the path names",
        "or via -z Zonename (for 'qu' and when working with resources).",
        " ",
        "Try 'help command' for more help on a specific command.",
        "'help qu' will explain additional options on the query.",
        ""
    };
    int i;
    for ( i = 0;; i++ ) {
        if ( strlen( msgs[i] ) == 0 ) {
            break;
        }
        printf( "%s\n", msgs[i] );
    }
    printReleaseInfo( "imeta" );
}

/*
Print either main usage/help information, or some more specific
information on particular commands.
 */
int
usage( char *subOpt ) {
    int i;
    if ( *subOpt == '\0' ) {
        usageMain();
    }
    else {
        if ( strcmp( subOpt, "add" ) == 0 ) {
            char *msgs[] = {
                " add -d|C|R|u Name AttName AttValue [AttUnits]  (Add new AVU triple)",
                "Add an AVU to a dataobj (-d), collection(-C), resource(-R), ",
                "or user(-u)",
                "Example: add -d file1 distance 12 miles",
                " ",
                "Admins can also use the command 'adda' (add as admin) to add metadata",
                "to any collection or dataobj; syntax is the same as 'add'.  Admins are",
                "also allowed to add user and resource metadata.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "adda" ) == 0 ) {
            char *msgs[] = {
                " adda -d|C|R|u Name AttName AttValue [AttUnits]  (Add as administrator)",
                "                                     (same as 'add' but bypasses ACLs)",
                " ",
                "Administrators (rodsadmin users) may use this command to add AVUs",
                "to any dataobj, collection, resource, or user.  The syntax is the same",
                "as 'imeta add'.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "addw" ) == 0 ) {
            char *msgs[] = {
                " addw -d Name AttName AttValue [AttUnits]  (Add new AVU triple)",
                "Add an AVU to a set of data-objects using wildcards to match",
                "the data-object names.",
                " ",
                "The character _ matches any single character and % matches any",
                "number of any characters.",
                " ",
                "Example: addw -d file% distance 12 miles",
                "would add the AVU to dataobjects in the current directory with names",
                "that start with 'file'.",
                " ",
                "Example2: addw -d /tempZone/home/rods/test/%/% distance 12 miles",
                "would add the AVU to all dataobjects in the 'test' collection or any",
                "subcollections under 'test'.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "rm" ) == 0 ) {
            char *msgs[] = {
                " rm  -d|C|R|u Name AttName AttValue [AttUnits] (Remove AVU)",
                "Remove an AVU from a dataobj (-d), collection(-C), resource(-R),",
                "or user(-u)",
                "Example: rm -d file1 distance 12 miles",
                "An AttUnits value must be included if it was when the AVU was added.",
                "Also see rmw for use of wildcard characters.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "rmw" ) == 0 ) {
            char *msgs[] = {
                " rmw  -d|C|R|u Name AttName AttValue [AttUnits] (Remove AVU, use Wildcard)",
                "Remove an AVU from a dataobj (-d), collection(-C), resource(-R), ",
                "or user(-u)",
                "An AttUnits value must be included if it was when the AVU was added.",
                "rmw is very similar to rm but using SQL wildcard characters, _ and %.",
                "The _ matches any single character and % matches any number of any",
                "characters.  Examples:",
                "  rmw -d file1 distance % %",
                " or ",
                "  rmw -d file1 distance % m% ",
                " ",
                "Note that if the attributes contain the characters '%' or '_', ",
                "the rmw command still do matching using them as wildcards, so you may",
                "need to use rm instead.",
                "Also see lsw.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "rmi" ) == 0 ) {
            char *msgs[] = {
                " rmi -d|C|R|u Name MetadataID (Remove AVU by MetadataID)",
                "Remove an AVU from a dataobj (-d), collection(-C), resource(-R),",
                "or user(-u) by name and metadataID from the catalog.  Intended for",
                "expert use.  There is no matching done to confirm correctness of",
                "a request (but permissions are still checked).  Use with caution.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "mod" ) == 0 ) {
            char *msgs[] = {
                " mod -d|C|R|u Name AttName AttValue [AttUnits] [n:Name] [v:Value] [u:Units]",
                "      (modify AVU; new name (n:), value(v:), and/or units(u:)",
                "Modify a defined AVU for the specified item (object)",
                "Example: mod -d file1 distance 14 miles v:27",
                " ",
                "Only the AVU associated with this object is modified.  Internally, the system",
                "removes the old AVU from the object and creates a new one (ensuring",
                "consistency), and performs a single 'commit' if all is valid.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "ls" ) == 0 ) {
            char *msgs[] = {
                " ls -d|C|R|u Name [AttName] (List existing AVUs for item Name)",
                " ls -ld Name [AttName]        (List in long format)",
                "List defined AVUs for the specified item",
                "Example: ls -d file1",
                "If the optional AttName is included, it is the attribute name",
                "you wish to list and only those will be listed.",
                "If the optional -l is used on dataObjects (-ld), the long format will",
                "be displayed which includes the time the AVU was set.",
                "Also see lsw.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }

        if ( strcmp( subOpt, "lsw" ) == 0 ) {
            char *msgs[] = {
                " lsw -d|C|R|u Name [AttName] (List existing AVUs, use Wildcards)",
                "List defined AVUs for the specified item",
                "Example: lsw -d file1",
                "If the optional AttName is included, it is the attribute name",
                "you wish to list, doing so using wildcard matching.",
                "For example: ls -d file1 attr%",
                "If the optional -l is used on dataObjects (-ld), the long format will",
                "be displayed which includes the time the AVU was set.",
                "Also see rmw and ls.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "set" ) == 0 ) {
            char *msgs[] = {
                " set -d|C|R|u Name AttName newValue [newUnits]  (assign a single value)",
                "Set the newValue (and newUnit) of an AVU of a dataobj (-d), collection(-C),",
                "     resource(-R) or user(-u).",
                " ",
                "'set' modifies an AVU if it exists, or creates one if it does not.",
                "If the AttName does not exist, or is used by multiple objects, the AVU for",
                "this object is added.",
                "If the AttName is used only by this one object, the AVU (row) is modified",
                "with the new values, reducing the database overhead (unused rows).",
                "Example: set -d file1 distance 12",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "qu" ) == 0 ) {
            char *msgs[] = {
                " qu -d|C|R|u AttName Op AttVal [...] (Query objects with matching AVUs)",
                "Query across AVUs for the specified type of item",
                "Example: qu -d distance '<=' 12",
                " ",
                "When querying dataObjects (-d) or collections (-C) additional conditions",
                "(AttName Op AttVal) may be given separated by 'and', for example:",
                " qu -d a = b and c '<' 10",
                "Or a single 'or' can be given for the same AttName, for example",
                " qu -d r '<' 5 or '>' 7",
                " ",
                "You can also query in numeric mode (instead of as strings) by adding 'n'",
                "in front of the test condition, for example:",
                " qu -d r 'n<' 123",
                "which causes it to cast the AVU column to numeric (decimal) in the SQL.",
                "In numeric mode, if any of the named AVU values are non-numeric, a SQL",
                "error will occur but this avoids problems when comparing numeric strings",
                "of different lengths.",
                " ",
                "Other examples:",
                " qu -d a like b%",
                "returns data-objects with attribute 'a' with a value that starts with 'b'.",
                " qu -d a like %",
                "returns data-objects with attribute 'a' defined (with any value).",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "cp" ) == 0 ) {
            char *msgs[] = {
                " cp -d|C|R|u -d|C|R|u Name1 Name2 (Copy AVUs from item Name1 to Name2)",
                "Example: cp -d -C file1 dir1",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        if ( strcmp( subOpt, "upper" ) == 0 ) {
            char *msgs[] = {
                " upper (Toggle between upper case mode for queries (qu)",
                "When enabled, the 'qu' queries will use the upper-case mode for the 'where'",
                "clause, so you can do a case-insensitive query (using an upper case literal)",
                "to compare against.  For example:",
                "  upper",
                "  qu -d A like B%",
                "will return all dataobjects with an AVU named 'A' or 'a' with a value that",
                "begins with 'b' or 'B'.",
                ""
            };
            for ( i = 0;; i++ ) {
                if ( strlen( msgs[i] ) == 0 ) {
                    return 0;
                }
                printf( "%s\n", msgs[i] );
            }
        }
        printf( "Sorry, either %s is an invalid command or the help has not been written yet\n",
                subOpt );
    }
    return 0;
}


