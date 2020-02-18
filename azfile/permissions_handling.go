package azfile

import (
	"errors"
)

func selectPermissionsPointers(permissionString, permissionKey, defaultString string) (strPtr, keyPtr *string, err error) {
	strPtr = &defaultString

	if permissionString != "" {
		strPtr = &permissionString
	}

	if permissionKey != "" {
		if strPtr == &defaultString {
			strPtr = nil
		} else if strPtr != nil {
			err = errors.New("only permission string OR permission key may be used")
			return
		}

		keyPtr = &permissionKey
	}

	return
}
