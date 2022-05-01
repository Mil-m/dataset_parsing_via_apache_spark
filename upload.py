import ftplib
import os


def ftp_upload(ftp_host: str, ftp_dir: str, outp_dir: str, key_dirs: set,
               re_upload: bool = False, silent: bool = True):

    def upload(ftp: str, key_dir: str, re_upload: bool, silent: bool):
        ftp_key_dir = os.path.join(ftp_dir, key_dir)
        print(f"Current FTP key directory is: {ftp_key_dir}")
        ftp.cwd(ftp_key_dir)

        files = ftp.nlst()
        file_names = [f for f in files if f.lower().endswith(".json")]

        outp_key_dir = os.path.join(outp_dir, key_dir)
        os.makedirs(outp_key_dir, exist_ok=True)
        for filename in file_names:
            if not silent:
                print(f"Uploading from {key_dir}: {filename}")
            outp_file = os.path.join(outp_dir, key_dir, filename)
            if os.path.exists(outp_file) and re_upload is False:
                pass
            else:
                with open(outp_file, "wb") as f:
                    ftp.retrbinary('RETR ' + filename, f.write)

    print(f"Output directory is: {outp_dir}")

    ftp = ftplib.FTP(ftp_host)
    ftp.login()

    for key_dir in key_dirs:
        upload(ftp=ftp, key_dir=key_dir, re_upload=re_upload, silent=silent)

    for key_dir in key_dirs:
        outp_key_dir = os.path.join(outp_dir, key_dir)
        print(f"Count of {outp_key_dir} files is: {len(os.listdir(outp_key_dir))}")

    ftp.quit()
    return 0
