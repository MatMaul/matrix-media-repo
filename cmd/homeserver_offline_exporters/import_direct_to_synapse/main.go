package main

import (
	"errors"
	"flag"
	"io"
	"math"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/t2bot/matrix-media-repo/cmd/homeserver_offline_exporters/_common"
	"github.com/t2bot/matrix-media-repo/common"
	"github.com/t2bot/matrix-media-repo/common/assets"
	"github.com/t2bot/matrix-media-repo/common/config"
	"github.com/t2bot/matrix-media-repo/common/rcontext"
	"github.com/t2bot/matrix-media-repo/common/runtime"
	"github.com/t2bot/matrix-media-repo/database"
	"github.com/t2bot/matrix-media-repo/homeserver_interop/synapse"
	"github.com/t2bot/matrix-media-repo/pipelines/pipeline_download"
	"github.com/t2bot/matrix-media-repo/util"
)

func main() {
	configPath := flag.String("config", "media-repo.yaml", "The path to the configuration")
	cfg := _common.InitExportPsqlFlatFile("Synapse", "media_store_path")

	config.Runtime.IsImportProcess = true // prevents us from creating media by accident
	config.Path = *configPath

	defer assets.Cleanup()
	assets.SetupMigrations(config.DefaultMigrationsPath)
	assets.SetupTemplates(config.DefaultTemplatesPath)

	logrus.Info("Starting up...")
	runtime.RunStartupSequence()

	logrus.Info("Starting export...")
	ctx := rcontext.Initial()
	mediaDb := database.GetInstance().Media.Prepare(ctx)
	previewsDb := database.GetInstance().UrlPreviews.Prepare(ctx)

	ctx.Log.Debug("Retrieving media records for entity")

	mxcsToIgnore := make(map[string]struct{})
	var exists = struct{}{}

	records, err := mediaDb.GetByOrigin(cfg.ServerName)
	if err != nil {
		panic(err)
	}

	allPreviewRelatedMxcs, err := previewsDb.GetAllPreviewRelatedMxcs()
	if err != nil {
		panic(err)
	}
	for _, mxc := range allPreviewRelatedMxcs {
		mxcsToIgnore[mxc] = exists
	}

	ctx.Log.Debug("Connecting to homeserver database...")
	hsDb, err := synapse.OpenDatabase(cfg.ConnectionString)
	if err != nil {
		panic(err)
	}

	ctx.Log.Infof("Exporting %d media records", len(records))
	for _, media := range records {
		mxc := util.MxcUri(media.Origin, media.MediaId)
		if _, ignore := mxcsToIgnore[mxc]; ignore {
			ctx.Log.Infof("Ignoring %s (url preview related media)", mxc)
			continue
		}
		if media.UserId == "" {
			ctx.Log.Infof("Ignoring %s (no associated user id)", mxc)
			continue
		}
		ctx.Log.Debugf("Downloading %s", mxc)
		_, s, err := pipeline_download.Execute(ctx, media.Origin, media.MediaId, pipeline_download.DownloadOpts{
			FetchRemoteIfNeeded: false,
			BlockForReadUntil:   10 * time.Minute,
			RecordOnly:          false,
			AuthProvided:        true, // it's for an export, so assume authentication
		})
		if errors.Is(err, common.ErrMediaQuarantined) {
			ctx.Log.Warnf("%s is quarantined and will not be included in the export", mxc)
			continue
		} else if errors.Is(err, common.ErrMediaNotYetUploaded) {
			ctx.Log.Debug("Media not uploaded yet - skipping")
			continue
		} else if err != nil {
			ctx.Log.Fatal(err)
			continue
		}

		if media.SizeBytes > math.MaxInt32 {
			ctx.Log.Warnf("%s is potentially too large for Synapse to handle. See https://github.com/matrix-org/synapse/issues/12023 for details.", mxc)
		}

		// For MediaID AABBCCDD :
		// $exportPath/local_content/AA/BB/CCDD
		directories := path.Join(cfg.ExportPath, "local_content", media.MediaId[0:2], media.MediaId[2:4])
		err = os.MkdirAll(directories, 0755)
		if err != nil {
			ctx.Log.Fatal(err)
			continue
		}
		filePath := path.Join(directories, media.MediaId[4:])

		mediaFileExists := false
		stat, _ := os.Stat(filePath)
		if stat != nil {
			if stat.Size() == media.SizeBytes {
				ctx.Log.Infof("%s has already been copied in Synapse media store", mxc)
				mediaFileExists = true
			}
		}

		alreadyInDb := false
		if ok, err := hsDb.HasMedia(media.MediaId); err != nil {
			ctx.Log.Fatal(err)
			return
		} else if ok {
			alreadyInDb = true
		}

		if !mediaFileExists {
			if alreadyInDb {
				ctx.Log.Infof("%s already exported in Synapse DB but not available (or wrong size) on disk", mxc)
			}

			file, err := os.Create(filePath)
			if err != nil {
				ctx.Log.Fatal(err)
				continue
			}

			ctx.Log.Infof("Copying %s", mxc)
			defer file.Close()
			_, err = io.Copy(file, s)
			if err != nil {
				ctx.Log.Errorf("Copy error: %s", err)
				continue
			}
		}

		if !alreadyInDb {
			if err = hsDb.InsertMedia(media.MediaId, media.ContentType, media.SizeBytes, media.CreationTs, media.UploadName, media.UserId); err != nil {
				ctx.Log.Fatalf("InsertMedia error: %s", err)
				continue
			}
		}
	}

	ctx.Log.Info("Done! If there's no warnings above, you're probably fine.")
}
