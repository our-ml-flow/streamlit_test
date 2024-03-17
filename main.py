import streamlit as st


def main():
    st.sidebar.success("☝️ 원하는 메뉴를 선택하세요")    
    
    st.markdown("""
## 6조 고래사냥꾼
                
                
### recommend
* 지갑 주소 유무에 따라 nft 컬렉션을 추천받을 수 있습니다. 

                
                
### wash trade
* 월별 비정상 거래에 대한 통계를 확인할 수 있습니다.  

                
                
### whale info
* 알고 싶은 고래 지갑 주소를 입력하면 지갑 속 컬렉션 구성을 확인할 수 있습니다. 

                
                
""")


if __name__ == '__main__':
    
    st.set_page_config(
        page_title = "고래 데이터 기반 NFT 추천 시스템",
        page_icon = "🐳"
    )

    st.title("🐳 고래 데이터 기반 NFT 추천 시스템")

    main()