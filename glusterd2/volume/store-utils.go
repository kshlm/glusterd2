package volume

import (
	"context"
	"encoding/json"

	"github.com/gluster/glusterd2/glusterd2/brick"
	"github.com/gluster/glusterd2/glusterd2/store"
	gderror "github.com/gluster/glusterd2/pkg/errors"

	"github.com/coreos/etcd/clientv3"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"go.opencensus.io/trace"
)

const (
	volumePrefix string = "volumes/"
)

// metadataFilter is a filter type
type metadataFilter uint32

// GetVolumes Filter Types
const (
	noKeyAndValue metadataFilter = iota
	onlyKey
	onlyValue
	keyAndValue
)

var (
	// AddOrUpdateVolumeFunc marshals to volume object and passes to store to add/update
	AddOrUpdateVolumeFunc = AddOrUpdateVolume
)

// AddOrUpdateVolume marshals to volume object and passes to store to add/update
func AddOrUpdateVolume(v *Volinfo) error {
	json, e := json.Marshal(v)
	if e != nil {
		log.WithError(e).Error("Failed to marshal the volinfo object")
		return e
	}

	_, e = store.Put(context.TODO(), volumePrefix+v.Name, string(json))
	if e != nil {
		log.WithError(e).Error("Couldn't add volume to store")
		return e
	}
	return nil
}

// GetVolume fetches the json object from the store and unmarshalls it into
// volinfo object
func GetVolume(name string) (*Volinfo, error) {
	var v Volinfo
	resp, e := store.Get(context.TODO(), volumePrefix+name)
	if e != nil {
		log.WithError(e).Error("Couldn't retrive volume from store")
		return nil, e
	}

	if resp.Count != 1 {
		return nil, gderror.ErrVolNotFound
	}

	if e = json.Unmarshal(resp.Kvs[0].Value, &v); e != nil {
		log.WithError(e).Error("Failed to unmarshal the data into volinfo object")
		return nil, e
	}
	return &v, nil
}

//DeleteVolume passes the volname to store to delete the volume object
func DeleteVolume(name string) error {
	_, e := store.Delete(context.TODO(), volumePrefix+name)
	return e
}

// GetVolumesList returns a map of volume names to their UUIDs
func GetVolumesList() (map[string]uuid.UUID, error) {
	resp, e := store.Get(context.TODO(), volumePrefix, clientv3.WithPrefix())
	if e != nil {
		return nil, e
	}

	volumes := make(map[string]uuid.UUID)

	for _, kv := range resp.Kvs {
		var vol Volinfo

		if err := json.Unmarshal(kv.Value, &vol); err != nil {
			log.WithError(err).WithField("volume", string(kv.Key)).Error("Failed to unmarshal volume")
			continue
		}

		volumes[vol.Name] = vol.ID
	}

	return volumes, nil
}

// getFilterType return the filter type for volume list/info
func getFilterType(filterParams map[string]string) metadataFilter {
	_, key := filterParams["key"]
	_, value := filterParams["value"]
	if key && !value {
		return onlyKey
	} else if value && !key {
		return onlyValue
	} else if value && key {
		return keyAndValue
	}
	return noKeyAndValue
}

//GetVolumes retrives the json objects from the store and converts them into
//respective volinfo objects
func GetVolumes(ctx context.Context, filterParams ...map[string]string) ([]*Volinfo, error) {
	if ctx != context.TODO() {
		var span *trace.Span
		ctx, span = trace.StartSpan(ctx, "volume.GetVolumes")
		defer span.End()
	}

	resp, e := store.Get(ctx, volumePrefix, clientv3.WithPrefix())
	if e != nil {
		return nil, e
	}

	var filterType metadataFilter
	if len(filterParams) == 0 {
		filterType = noKeyAndValue
	} else {
		filterType = getFilterType(filterParams[0])
	}

	var volumes []*Volinfo

	for _, kv := range resp.Kvs {
		var vol Volinfo

		if err := json.Unmarshal(kv.Value, &vol); err != nil {
			log.WithError(err).WithField("volume", string(kv.Key)).Error("Failed to unmarshal volume")
			continue
		}
		switch filterType {

		case onlyKey:
			if _, keyFound := vol.Metadata[filterParams[0]["key"]]; keyFound {
				volumes = append(volumes, &vol)
			}
		case onlyValue:
			for _, value := range vol.Metadata {
				if value == filterParams[0]["value"] {
					volumes = append(volumes, &vol)
				}
			}
		case keyAndValue:
			if value, keyFound := vol.Metadata[filterParams[0]["key"]]; keyFound {
				if value == filterParams[0]["value"] {
					volumes = append(volumes, &vol)
				}
			}
		default:
			volumes = append(volumes, &vol)

		}
	}

	return volumes, nil
}

// GetAllBricksInCluster returns all bricks in the cluster. These bricks
// belong to different volumes.
func GetAllBricksInCluster() ([]brick.Brickinfo, error) {

	volumes, err := GetVolumes(context.TODO())
	if err != nil {
		return nil, err
	}

	var bricks []brick.Brickinfo
	for _, volinfo := range volumes {
		bricks = append(bricks, volinfo.GetBricks()...)
	}

	return bricks, nil
}

// AreReplicateVolumesRunning checks if all replicate and disperse volumes are stopped.
// The volume being acted upon is excluded from this check and
// the volume ID of that volume needs to be volume passed as an argument.
func AreReplicateVolumesRunning(skipVolID uuid.UUID) (bool, error) {
	volumes, e := GetVolumes(context.TODO())
	if e != nil {
		return false, e
	}
	for _, v := range volumes {
		if uuid.Equal(v.ID, skipVolID) {
			continue
		}
		if (v.Type == Replicate || v.Type == Disperse || v.Type == DistReplicate || v.Type == DistDisperse) && v.State == VolStarted {
			return true, nil
		}
	}

	return false, nil
}

//Exists check whether a given volume exist or not
func Exists(name string) bool {
	resp, e := store.Get(context.TODO(), volumePrefix+name)
	if e != nil {
		return false
	}

	return resp.Count == 1
}

// CheckBrickExistence checks if a brick is part of a host in the volume
func CheckBrickExistence(volinfo *Volinfo, hostname, brickname string) error {
	bricks := volinfo.GetBricks()
	hostFound := false
	for _, b := range bricks {
		if b.Hostname == hostname {
			hostFound = true
		}
	}
	if hostFound {
		for _, b := range bricks {
			if b.Path == brickname && b.Hostname == hostname {
				return nil
			}
		}
		return gderror.ErrInvalidBrickName
	}

	return gderror.ErrInvalidHostName
}
