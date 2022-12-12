import Extract

link_folder_dataset = 'https://drive.google.com/drive/folders/1UNLGgdPmSqzmJXitIV6oriXyJUtLRA3s?usp=share_link'

def main():
    print("Downloading folder 'Datasets_mineria'...")

    # Extrae datasets de una carpeta de Google Drive.
    Extract.extract_folder_drive(link_folder_dataset)
    # Extrae datasets de una web utilizando web scrapping y robótica.
    Extract.extract_web_scrapping()

    print("Folder downloaded successfully.")

if __name__ == "__main__": 
    main()