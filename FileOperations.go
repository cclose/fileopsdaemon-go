package FileDaemon

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	nfs4 "github.com/cclose/libnfs4acl-go"
	"golang.org/x/crypto/blake2b"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
	"strings"
	"github.com/karrick/godirwalk"
	"os/exec"
	//"fmt"
	"fmt"
	"bytes"
	"github.com/cclose/go-utils/pathext"
)

const ( //for doChecksum
	CHKSUM_REPLY_COUNT        = 1
	CHKSUM_REPLY_IDX          = 0
	CHKSUM_PARAM_COUNT        = 2
	CHKSUM_PARAM_ALGOR_IDX    = 0
	CHKSUM_PARAM_FILEPATH_IDX = 1
)

func (worker *Worker) doChecksum(params []string) (reply []string, err error) {
	reply = make([]string, CHKSUM_REPLY_COUNT, CHKSUM_REPLY_COUNT) //we return 1 message chunk... the checksum!

	if len(params) != CHKSUM_PARAM_COUNT {
		err = errors.New("Incorrect number of parameters to checksum. Expected " +
			string(CHKSUM_PARAM_COUNT) + " Got " + string(len(params)))
		return
	}

	checkSumAlgor /*e*/ := params[CHKSUM_PARAM_ALGOR_IDX]
	filePath := params[CHKSUM_PARAM_FILEPATH_IDX]

	// Get bytes from file
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return
	}

	// Hash the file and output results
	var checkSum string
	switch checkSumAlgor {
	case "md5":
		chkSumBytes := md5.Sum(data)
		checkSum = hex.EncodeToString(chkSumBytes[:])
		break
	case "blake2b":
		chkSumBytes := blake2b.Sum256(data)
		checkSum = hex.EncodeToString(chkSumBytes[:])
		break

	default:
		err = errors.New("Unsupported checksum algorithm " + checkSumAlgor)
	}

	//if we have no error, then convert our byte stream into a string
	if err == nil {
		reply[CHKSUM_REPLY_IDX] = checkSum
	}

	return
}

//Basic ACL mask sets
// These are useful for toggling specific sets of ACLs, or for ORing together to make a full access mask
const (
	//These Masks specify ACL bits to toggle
	ACL_MASK_READ    = nfs4.NFS4_ACE_READ_DATA
	ACL_MASK_EXECUTE = nfs4.NFS4_ACE_EXECUTE
	ACL_MASK_WRITE   = nfs4.NFS4_ACE_WRITE_DATA | nfs4.NFS4_ACE_APPEND_DATA | nfs4.NFS4_ACE_WRITE_ATTRIBUTES |
		nfs4.NFS4_ACE_WRITE_NAMED_ATTRS
		//Addition Write privileges for empowered suers (file owner/group)
	ACL_MASK_OGWRITE_PART = nfs4.NFS4_ACE_DELETE | nfs4.NFS4_ACE_DELETE_CHILD
	//Full set of OG Write ACLs
	ACL_MASK_OGWRITE_FULL = ACL_MASK_WRITE | ACL_MASK_OGWRITE_PART
	//Macro of both Read and Write
	ACL_MASK_RX = ACL_MASK_READ | ACL_MASK_EXECUTE
)

//Approved ACLs access Masks
// These are full access Masks that are used to set very specific permissions
const (
	//Base ACL == tncy
	ACL_SET_BASE = nfs4.NFS4_ACE_READ_ATTRIBUTES | nfs4.NFS4_ACE_READ_NAMED_ATTRS | nfs4.NFS4_ACE_READ_ACL |
		nfs4.NFS4_ACE_SYNCHRONIZE
	//Lock ACL == r{base} == {base} | r
	ACL_SET_LOCK = ACL_SET_BASE | ACL_MASK_READ
	//Read ACL == rx{base} == {lock} | x
	ACL_SET_READ = ACL_SET_LOCK | ACL_MASK_EXECUTE
	//Write Permissions for Everyone == 7 == rxwaT{base}N == {read} | waTN
	ACL_SET_EWRITE = ACL_SET_READ | ACL_MASK_WRITE
	//Write Permissions for Owner or Group == 7 == rxwadDT{base}N == {ewrite} | dD
	ACL_SET_OGWRITE = ACL_SET_EWRITE | ACL_MASK_OGWRITE_PART
)

//ACL Flags
const (
	//directory flags
	ACL_FLAG_CLEAR = 0
	ACL_FLAG_DIR = nfs4.NFS4_ACE_FILE_INHERIT_ACE | nfs4.NFS4_ACE_DIRECTORY_INHERIT_ACE
	ACL_FLAG_GROUP = nfs4.NFS4_ACE_IDENTIFIER_GROUP
	ACL_FLAG_DIRGROUP = ACL_FLAG_DIR | ACL_FLAG_GROUP
)

const ACL_DOMAINUSERS_WHONAME = "domain users@x-es.com"

const (
	CHMOD_PARAM_COUNT = 3
	//CHMOD_REPLY_COUNT  = 0
	CHMOD_PARAM_MODE_IDX      = 0
	CHMOD_PARAM_RECURSIVE_IDX = 1
	CHMOD_PARAM_FILEPATH_IDX  = 2
)

func (worker *Worker) doChmod(params []string) (reply []string, err error) {
	//reply = make([]string, CHMOD_REPLY_COUNT, CHMOD_REPLY_COUNT) //we return 0 message chunks
	if len(params) != CHMOD_PARAM_COUNT {
		err = errors.New("Incorrect number of parameters to chmod. Expected " +
			strconv.Itoa(CHMOD_PARAM_COUNT) + " Got " + strconv.Itoa(len(params)))
		return
	}

	worker.logMessage("doing chmod request")

	filePath := params[CHMOD_PARAM_FILEPATH_IDX]
	mode := params[CHMOD_PARAM_MODE_IDX]
	recursive, err := strconv.ParseBool(params[CHMOD_PARAM_RECURSIVE_IDX])
	if err != nil {
		return
	}

	//Check if file exists
	fi, err := os.Stat(filePath)
	if err != nil {
		return
	}

	additiveMode := false
	var octalPerms os.FileMode //these are just ints, but prefix a 0### so it's Octal
	var ownerMask, groupMask, everyoneMask uint32
	//determine ACL list or Octal permissions
	//we use string modes to better control what permissions can be granted
	switch mode {
	case "lock":
		everyoneMask = ACL_SET_LOCK
		groupMask = ACL_SET_LOCK
		ownerMask = ACL_SET_LOCK
		octalPerms = 0444
		break
	case "read":
		everyoneMask = ACL_SET_READ
		groupMask = ACL_SET_READ
		ownerMask = ACL_SET_READ
		octalPerms = 0555
		break
	case "owrite":
		everyoneMask = ACL_SET_READ
		groupMask = ACL_SET_READ
		ownerMask = ACL_SET_OGWRITE
		octalPerms = 0755
		break
	case "ogwrite":
		everyoneMask = ACL_SET_READ
		groupMask = ACL_SET_OGWRITE
		ownerMask = ACL_SET_OGWRITE
		octalPerms = 0775
		break
	case "write":
		everyoneMask = ACL_SET_EWRITE
		groupMask = ACL_SET_OGWRITE
		ownerMask = ACL_SET_OGWRITE
		octalPerms = 0777
		break
	case "aread":
		additiveMode = true
		octalPerms = 0555 //Set up a bit mask we will apply
		break

	default:
		err = errors.New("unsupported file mode " + mode)
		return
	}

	notNFS4 := false
	if recursive && fi.IsDir() { //must be a dir to walk
		worker.logMessage("recursive")
		count := 0
		//godirwalk will walk the directory tree in parallel, calling the below callback
		//if WILL visit the root node, so no additional call is needed
		var fileACL, dirACL *nfs4.NFS4ACL //containers for override ACLs
		err = godirwalk.Walk(filePath, &godirwalk.Options{
			Unsorted: true,
			Callback: func(subFilePath string, de *godirwalk.Dirent) error {
				count++
				if de.IsDir() {
					dirACL, err = worker.executeChmod(subFilePath, additiveMode, octalPerms, everyoneMask, groupMask, ownerMask, de.IsDir(), &notNFS4, dirACL)
				} else {
					fileACL, err = worker.executeChmod(subFilePath, additiveMode, octalPerms, everyoneMask, groupMask, ownerMask, de.IsDir(), &notNFS4, fileACL)
				}

				return err
			},
		})
	} else {
		worker.logMessage("not recursive")

		//our function expects a Dirent
		_, err = worker.executeChmod(filePath, additiveMode, octalPerms, everyoneMask, groupMask, ownerMask, fi.IsDir(), &notNFS4, nil)
	}


	return
}

func (worker Worker)executeChmod(filePath string, additiveMode bool, octalPerms os.FileMode, everyoneMask, groupMask, ownerMask uint32, isDir bool, notNFS4 *bool, overrideACL *nfs4.NFS4ACL) (acl *nfs4.NFS4ACL, err error) {
	//Attempt the Chmod using nfs4
	if !*notNFS4 {
		worker.logMessage("trying nfs4")
		if overrideACL != nil {
			acl = overrideACL
		} else {
			acl, err = nfs4.GetAcl(filePath, isDir) //statless variant
		}
	}

	if err == nil && !*notNFS4 {
		worker.logMessage("nfs4")
		if overrideACL == nil {
			//update ACLs
			if additiveMode {
				//OR in the RX mask on all ACEs
				acl.ApplyAccessMask(ACL_MASK_RX)
			} else {
				//wipe out the current ACE list... we'll set out own
				acl.ClearACEs()

				if isDir {
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_DIR,      everyoneMask, nfs4.NFS4_ACL_WHO_EVERYONE_STRING)
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_DIRGROUP, groupMask,    nfs4.NFS4_ACL_WHO_GROUP_STRING)
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_DIRGROUP, groupMask,    ACL_DOMAINUSERS_WHONAME)
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_DIR,      ownerMask,    nfs4.NFS4_ACL_WHO_OWNER_STRING)
				} else {
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_CLEAR, everyoneMask, nfs4.NFS4_ACL_WHO_EVERYONE_STRING)
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_GROUP, groupMask,    nfs4.NFS4_ACL_WHO_GROUP_STRING)
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_GROUP, groupMask,    ACL_DOMAINUSERS_WHONAME)
					acl.AddACE(nfs4.NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE, ACL_FLAG_CLEAR, ownerMask,    nfs4.NFS4_ACL_WHO_OWNER_STRING)
				}
			}
		}

		//commit to filesystem
		err = nfs4.SetACL(filePath, acl) //statless variant

	} else {
		worker.logMessage("not nfs4")
		//we failed, see if it's because the filesystem is not nfs4
		if err != nil && err.Error() == nfs4.ERROR_NFS4_NOT_SUPPORTED {
			worker.logMessage("not nfs4 msg")
			if !*notNFS4 {
				worker.logMessage("unsetting nfs4")
				*notNFS4 = true
			}
		}
		if *notNFS4 {
			worker.logMessage("is not nfs4")
			if additiveMode {
				worker.logMessage("additive chmod")
				//in additive mode, we bitwise OR our mask with the existing mode
				fileStat, _ := os.Stat(filePath) //we must stat to get that mode
				err = os.Chmod(filePath, fileStat.Mode()|octalPerms)
			} else {
				worker.logMessage("regular chmod: " + filePath + "|" + string(octalPerms))
				err = os.Chmod(filePath, octalPerms)
				if err != nil {
					worker.logMessage("chmod errored " + err.Error())
				}
			}
		}

	}

	return
}

const (
	CHOWN_PARAM_COUNT = 3
	//CHOWN_REPLY_COUNT  = 0
	CHOWN_PARAM_OWNER_IDX = 0
	CHOWN_PARAM_RECURSIVE_IDX = 1
	CHOWN_PARAM_FILEPATH_IDX    = 2
	CHOWN_OWNER_SEP             = ":"
	CHOWN_OWNERSTRING_OWNER_IDX = 0
	CHOWN_OWNERSTRING_GROUP_IDX = 1
)

func (worker *Worker) doChown(params []string) (reply []string, err error) {
	if len(params) != CHOWN_PARAM_COUNT {
		err = errors.New("Incorrect number of parameters to chown. Expected " +
			strconv.Itoa(CHOWN_PARAM_COUNT) + " Got " + strconv.Itoa(len(params)))
		return
	}

	filePath := params[CHOWN_PARAM_FILEPATH_IDX]
	ownerName := params[CHOWN_PARAM_OWNER_IDX]
	recursive, err := strconv.ParseBool(params[CHOWN_PARAM_RECURSIVE_IDX])
	if err != nil {
		return
	}

	//Check if file exists
	fi, err := os.Stat(filePath)
	if err != nil {
		return
	}

	ownerParts := strings.Split(ownerName, CHOWN_OWNER_SEP)
	if 0 == len(ownerParts) || len(ownerParts) > 2 {
		err = errors.New("invalid owner string: " + ownerName)
		return
	}

	var groupUid int
	owner, err := user.Lookup(ownerParts[CHOWN_OWNERSTRING_OWNER_IDX])
	if err != nil {
		return
	}
	ownerUid, err := strconv.Atoi(owner.Uid)
	if err != nil {
		return
	}

	if len(ownerParts) > 1 {
		group, err := user.LookupGroup(ownerParts[CHOWN_OWNERSTRING_GROUP_IDX])
		if err != nil {
			return reply, err //naked return here trips "err is shadowed during return"
		}
		groupUid, err = strconv.Atoi(group.Gid)
		if err != nil {
			return reply, err //naked return here trips "err is shadowed during return"
		}
	}

	if recursive && fi.IsDir() { //must be a dir to walk
		//godirwalk will walk the directory tree in parallel, calling the below callback
		//if WILL visit the root node, so no additional call is needed
		err = godirwalk.Walk(filePath, &godirwalk.Options{
			Unsorted: true,
			Callback: func(subFilePath string, de *godirwalk.Dirent) error {
				return os.Chown(subFilePath, ownerUid, groupUid)
			},
		})

	} else {
		err = os.Chown(filePath, ownerUid, groupUid)
	}

	return
}

const (
	COPY_PARAM_COUNT = 3
	//COPY_REPLY_COUNT  = 0
	COPY_PARAM_RECURSIVE_IDX = 0
	COPY_PARAM_SRCFILEPATH_IDX    = 1
	COPY_PARAM_DSTFILEPATH_IDX    = 2
)
func (worker *Worker) doCopy(params []string) (reply []string, err error) {
	if len(params) != COPY_PARAM_COUNT {
		err = errors.New("Incorrect number of parameters to copy. Expected " +
			strconv.Itoa(COPY_PARAM_COUNT) + " Got " + strconv.Itoa(len(params)))
		return
	}

	srcFilePath := params[COPY_PARAM_SRCFILEPATH_IDX]
	dstFilePath := params[COPY_PARAM_DSTFILEPATH_IDX]
	recursive, err := strconv.ParseBool(params[COPY_PARAM_RECURSIVE_IDX])
	if err != nil {
		return
	}

	//Check if file exists
	_, err = os.Stat(srcFilePath)
	if err != nil {
		return
	}

	//Check if file exists
	_, err = os.Stat(dstFilePath)
	if err == nil {
		err = errors.New("destination FilePath already exists")
		return
	}

	//Shell out to cp
	cmdText := "cp"
	args := make([]string, 0, 3)
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, srcFilePath)
	args = append(args, dstFilePath)
	cmd := exec.Command(cmdText, args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		err = errors.New(fmt.Sprint(err) + ": " + stderr.String() + "\n")
	}

	return
}


const (
	MKDIR_PARAM_COUNT = 2
	MKDIR_REPLY_COUNT  = 1
	MKDIR_REPLY_SUBDIR_IDX = 0
	MKDIR_PARAM_MODE_IDX    = 0
	MKDIR_PARAM_FILEPATH_IDX    = 1
)
func (worker *Worker) doMkdir(params []string) (reply []string, err error) {
	reply = make([]string, MKDIR_REPLY_COUNT, MKDIR_REPLY_COUNT) //we return 1 message chunk... the checksum!
	if len(params) != MKDIR_PARAM_COUNT {
		err = errors.New("Incorrect number of parameters to mkdir. Expected " +
			strconv.Itoa(MKDIR_PARAM_COUNT) + " Got " + strconv.Itoa(len(params)))
		return
	}

	modeInt, _ := strconv.Atoi(params[MKDIR_PARAM_MODE_IDX])
	mode := os.FileMode(modeInt)
	filePath := params[MKDIR_PARAM_FILEPATH_IDX]

	//Yes, os.MkdirAll can handle this but we want to know what subpaths actaully get made
	// and verify their ACLs when done, so it's faster to do it this way
	dirAtoms, _ := pathext.SplitAll(filePath)

	//look for the part of the path that exists. This might be none
	var existingRoot string
	var newDirs []string
	rootFound := false
	//work backwards, checking for directories
	for i := len(dirAtoms) -1 ; i >= 0 && !rootFound; i-- {
		existingRoot = strings.Join(dirAtoms[:i+1], string(os.PathSeparator))
		fi, fiErr := os.Stat(existingRoot)
		if fiErr == nil {
			if fi.IsDir() {
				rootFound = true
			} else {
				err = errors.New(fmt.Sprintf("Cannot mkdir %s: subpath %s exists but is not a directory!",
					filePath, existingRoot))
				return
			}
		} else if os.IsNotExist(fiErr) {
			newDirs = append(newDirs, dirAtoms[i])
		}
	}

	dirPath := ""
	var acl *nfs4.NFS4ACL
	isACL := true
	//if we found a root, get the ACls from it so we can clone them
	if rootFound {
		dirPath = existingRoot
		//if the root is existing, get the permissions of it
		var getAclErr error
		acl, getAclErr = nfs4.GetAcl(dirPath, true)
		if getAclErr != nil && getAclErr.Error() == nfs4.ERROR_NFS4_NOT_SUPPORTED { //not nfs4, use regular chmod
			isACL = false
		}
	}
	//now work forwards from the existing root, adding new directories
	var createdSubpath string
	for j := len(newDirs) - 1; j >= 0; j-- {
		//add the new
		dirPath += string(os.PathSeparator) + newDirs[j]
		mkdirerr := os.Mkdir(dirPath, mode)
		if mkdirerr != nil {
			err = mkdirerr
			return
		}
		//on the first iteration, remember the first subpath needing creation
		if createdSubpath == "" {
			createdSubpath = dirPath
		}
		if rootFound && acl != nil {
			//TODO if we had a way to compare ACLs
			//  get dirPath ACL and if dirpathACL != Acl
			worker.executeChmod(dirPath, false, os.FileMode(0), uint32(0), uint32(0), uint32(0), true, &isACL, acl)
		}

	}

	reply[MKDIR_REPLY_SUBDIR_IDX] = createdSubpath

	return
}

const (
	MOVE_PARAM_COUNT = 2
	//MOVE_REPLY_COUNT  = 0
	MOVE_PARAM_SRCFILEPATH_IDX    = 0
	MOVE_PARAM_DSTFILEPATH_IDX    = 1
)
func (worker *Worker) doMove(params []string) (reply []string, err error) {
	if len(params) != MOVE_PARAM_COUNT {
		err = errors.New("Incorrect number of parameters to move. Expected " +
			strconv.Itoa(MOVE_PARAM_COUNT) + " Got " + strconv.Itoa(len(params)))
		return
	}

	srcFilePath := params[MOVE_PARAM_SRCFILEPATH_IDX]
	dstFilePath := params[MOVE_PARAM_DSTFILEPATH_IDX]

	//Check if file exists
	_, err = os.Stat(srcFilePath)
	if err != nil {
		return
	}

	//Check if file exists
	_, err = os.Stat(dstFilePath)
	if err == nil {
		err = errors.New("destination FilePath already exists")
		return
	}

	//Shell out to MV
	cmdText := "mv"
	args := []string{srcFilePath, dstFilePath}
	cmd := exec.Command(cmdText, args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		err = errors.New(fmt.Sprint(err) + ": " + stderr.String() + "\n")
	}

	return
}

const (
	REMOVE_PARAM_COUNT = 3
	//REMOVE_REPLY_COUNT  = 0
	REMOVE_PARAM_RECURSIVE_IDX = 0
	REMOVE_PARAM_IGNORE_MISSING_IDX = 1
	REMOVE_PARAM_FILEPATH_IDX    = 2
)
func (worker *Worker) doRemove(params []string) (reply []string, err error) {
	if len(params) != REMOVE_PARAM_COUNT {
		err = errors.New("Incorrect number of parameters to remove. Expected " +
			strconv.Itoa(REMOVE_PARAM_COUNT) + " Got " + strconv.Itoa(len(params)))
		return
	}

	filePath := params[REMOVE_PARAM_FILEPATH_IDX]
	recursive, err := strconv.ParseBool(params[REMOVE_PARAM_RECURSIVE_IDX])
	ignoreMissing, err := strconv.ParseBool(params[REMOVE_PARAM_IGNORE_MISSING_IDX])

	//Check if file exists
	_, statErr := os.Stat(filePath)
	if statErr != nil {
		//we return no error because they want the path removed and it doesn't exists... sooo... good?
		if !ignoreMissing {
			//unless they do care...
			err = statErr
		}
		return
	}

	if recursive {
		err = os.RemoveAll(filePath)
	} else {
		err = os.Remove(filePath)
	}

	return
}
