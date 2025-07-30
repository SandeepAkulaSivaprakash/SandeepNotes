Sub AddCheckBoxesAligned()
    Dim Rng As Range
    Dim SelectionRng As Range
    Dim WSHEET As Worksheet
    Dim cb As CheckBox

    On Error Resume Next
    xTitleId = "Select Range"

    Set SelectionRng = Application.InputBox("Range", xTitleId, Selection.Address, Type:=8)
    Set WSHEET = Application.ActiveSheet

    Application.ScreenUpdating = False

    For Each Rng In SelectionRng
        Set cb = WSHEET.CheckBoxes.Add(0, 0, 1, 1) ' placeholder

        With cb
            .Top = Rng.Top + (Rng.Height - .Height) / 2   ' vertical centering
            .Left = Rng.Left + (Rng.Width - .Width) / 2   ' horizontal centering
            .Height = Rng.Height * 0.9                    ' slightly smaller than cell
            .Width = Rng.Width * 0.9
            .Caption = ""                                 ' no text on checkbox
            .LinkedCell = Rng.Address
        End With
    Next

    Application.ScreenUpdating = True
End Sub
